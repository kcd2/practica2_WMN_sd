// Archivo: WM_WS_E.c
// Compilación: gcc WM_WS_E.c -o WM_WS_E -lpthread -lrdkafka

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

char *kafka_broker;
char *monitor_ip;
char *monitor_puerto;
char *id_estacion;

int s_monitor = -1;
volatile int estado_fuga = 0;   // 0 = OK, 1 = FUGA (KO)
volatile int abortar_riego = 0; // 1 = Parada ordenada por Central o manual (sin ser fuga)
volatile int regando = 0;
volatile int ejecutando = 1;

void finalizar(int sig)
{
    printf("\n[ENGINE] Apagando motor de la estación...\n");
    ejecutando = 0;
    if (s_monitor != -1) close(s_monitor);
    exit(0);
}

/* =========================================================================
 * HILO CLIENTE LOCAL CON SOPORTE DNS PARA CONECTAR AL MONITOR
 * ========================================================================= */
void *hilo_conexion_monitor(void *arg)
{
    struct sockaddr_in direccion;
    struct hostent *he;
    int n, enviados;
    char buffer[128];

    while (ejecutando)
    {
        s_monitor = socket(AF_INET, SOCK_STREAM, 0);
        if (s_monitor == -1)
        {
            sleep(2);
            continue;
        }

        he = gethostbyname(monitor_ip);
        if (he == NULL)
        {
            fprintf(stderr, "[ENGINE] Error resolviendo host del monitor: %s\n", monitor_ip);
            close(s_monitor);
            s_monitor = -1;
            sleep(2);
            continue;
        }

        direccion.sin_family = AF_INET;
        direccion.sin_port = htons(atoi(monitor_puerto));
        memcpy(&direccion.sin_addr, he->h_addr_list[0], he->h_length);

        if (connect(s_monitor, (struct sockaddr *)&direccion, sizeof(direccion)) == -1)
        {
            close(s_monitor);
            s_monitor = -1;
            sleep(2);
            continue;
        }

        printf("[ENGINE] Conectado al Monitor local en %s:%s\n", monitor_ip, monitor_puerto);

        while (ejecutando)
        {
            n = read(s_monitor, buffer, sizeof(buffer) - 1);
            if (n <= 0)
            {
                printf("[ENGINE] Conexion perdida con el Monitor local. Reconectando...\n");
                close(s_monitor);
                s_monitor = -1;
                break;
            }

            buffer[n] = '\0';
            if (strstr(buffer, "DETENER_FUGA") != NULL)
            {
                estado_fuga = 1;
            }
            else if (strstr(buffer, "RESTABLECER") != NULL)
            {
                estado_fuga = 0;
            }

            const char *latido = estado_fuga ? "KO\n" : "OK\n";
            enviados = write(s_monitor, latido, strlen(latido));
            if (enviados <= 0)
            {
                printf("[ENGINE] Error al enviar estado al Monitor local. Reconectando...\n");
                close(s_monitor);
                s_monitor = -1;
                break;
            }
        }
    }
    return NULL;
}

/* =========================================================================
 * HILO DE SIMULACIÓN DEL CICLO DE RIEGO, TELEMETRÍA Y FIN DE RIEGO
 * ========================================================================= */
void *hilo_ciclo_riego(void *arg)
{
    if (estado_fuga)
    {
        printf("[ENGINE] Orden de riego rechazada: La estación tiene una FUGA activa.\n");
        return NULL;
    }
    if (regando)
    {
        printf("[ENGINE] La estación ya se encuentra regando.\n");
        return NULL;
    }

    regando = 1;
    abortar_riego = 0;
    double volumen_total = 0.0;
    int segundos = 0;

    char errstr[512];
    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", kafka_broker, errstr, sizeof(errstr));
    rd_kafka_t *rk_prod = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    rd_kafka_topic_t *topic_patron = rd_kafka_topic_new(rk_prod, "wm_peticiones", NULL);

    printf("\n======================================================\n");
    printf("[ENGINE %s] >>> ABRIENDO ELECTROVÁLVULA - INICIANDO RIEGO <<<\n", id_estacion);
    printf("======================================================\n");

    while (segundos < 10 && !estado_fuga && !abortar_riego && ejecutando)
    {
        sleep(1);
        segundos++;
        double caudal_lps = 5.5 + ((rand() % 10) - 5) * 0.1;
        double caudal_lpm = caudal_lps * 60.0;
        volumen_total += caudal_lps;

        printf("[ENGINE %s] Regando... %02ds/10s | Caudal: %.2f L/min | Volumen: %.2f L\n",
               id_estacion, segundos, caudal_lpm, volumen_total);

        char payload_tel[256];
        snprintf(payload_tel, sizeof(payload_tel), "TELEMETRIA#%s#%.2f#%.2f", id_estacion, caudal_lpm, volumen_total);
        rd_kafka_produce(topic_patron, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                         payload_tel, strlen(payload_tel), NULL, 0, NULL);
        rd_kafka_flush(rk_prod, 500);
    }

    regando = 0;

    char payload_fin[256];
    snprintf(payload_fin, sizeof(payload_fin), "FIN_RIEGO#%s#%.2f#%d", id_estacion, volumen_total, segundos);
    rd_kafka_produce(topic_patron, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                     payload_fin, strlen(payload_fin), NULL, 0, NULL);
    rd_kafka_flush(rk_prod, 1000);

    rd_kafka_topic_destroy(topic_patron);
    rd_kafka_destroy(rk_prod);

    if (estado_fuga)
    {
        printf("[ENGINE %s] Riego abortado por FUGA. Volumen suministrado: %.2f L\n", id_estacion, volumen_total);
    }
    else if (abortar_riego)
    {
        printf("[ENGINE %s] Riego detenido por orden de parada/bloqueo. Volumen suministrado: %.2f L\n", id_estacion, volumen_total);
    }
    else
    {
        printf("[ENGINE %s] Riego finalizado correctamente. Volumen total: %.2f L\n", id_estacion, volumen_total);
    }

    return NULL;
}

void lanzar_riego_async()
{
    pthread_t th_riego;
    pthread_create(&th_riego, NULL, hilo_ciclo_riego, NULL);
    pthread_detach(th_riego);
}

/* =========================================================================
 * CONSUMIDOR DE KAFKA (ESCUCHA ÓRDENES DE RIEGO DE LA CENTRAL)
 * ========================================================================= */
void *hilo_consumidor_kafka(void *arg)
{
    char errstr[512];
    char group_id[128];
    snprintf(group_id, sizeof(group_id), "grupo_engine_%s", id_estacion);

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", kafka_broker, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr));

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(rk);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, "wm_ordenes_ws", RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    printf("[ENGINE] Escuchando órdenes de riego en Kafka (topic: wm_ordenes_ws)...\n");

    while (ejecutando)
    {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, 1000);
        if (!msg) continue;

        if (!msg->err && msg->payload)
        {
            char payload[512];
            snprintf(payload, sizeof(payload), "%.*s", (int)msg->len, (char *)msg->payload);

            if (strstr(payload, id_estacion) != NULL)
            {
                if (strstr(payload, "INICIAR_RIEGO") != NULL)
                {
                    printf("[ENGINE] Orden de Kafka recibida: %s\n", payload);
                    lanzar_riego_async();
                }
                else if (strstr(payload, "DETENER_RIEGO") != NULL)
                {
                    printf("[ENGINE] ¡Orden de DETENER_RIEGO recibida desde Central! Deteniendo válvula...\n");
                    abortar_riego = 1;
                }
            }
        }
        rd_kafka_message_destroy(msg);
    }

    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
    return NULL;
}

int main(int argc, char *argv[])
{
    setbuf(stdout, NULL);
    signal(SIGINT, finalizar);

    if (argc != 5)
    {
        fprintf(stderr, "Uso: %s <ip_broker_kafka> <ip_monitor_local> <puerto_monitor_local> <id_estacion>\n", argv[0]);
        fprintf(stderr, "Ejemplo : %s 127.0.0.1:9092 127.0.0.1 9090 WS-01\n", argv[0]);
        return 1;
    }

    kafka_broker = argv[1];
    monitor_ip = argv[2];
    monitor_puerto = argv[3];
    id_estacion = argv[4];

    pthread_t th_mon;
    pthread_create(&th_mon, NULL, hilo_conexion_monitor, NULL);

    pthread_t th_kafka;
    pthread_create(&th_kafka, NULL, hilo_consumidor_kafka, NULL);

    char opcion[16];
    while (ejecutando)
    {
        printf("\n--- MENU ENGINE (%s) ---\n", id_estacion);
        printf("Salud fisica: %s | Valvula: %s\n", estado_fuga ? "CON FUGA (KO)" : "OK", regando ? "REGANDO" : "CERRADA");
        printf("1. Provocar FUGA de agua (envia KO al Monitor)\n");
        printf("2. Reparar FUGA (envia OK al Monitor)\n");
        printf("3. Iniciar riego manualmente en la estacion\n");
        printf("4. Finalizar riego manualmente en la estacion\n");
        printf("Seleccione opcion: ");

        if (fgets(opcion, sizeof(opcion), stdin) == NULL)
        {
            sleep(1);
            continue;
        }

        int op = atoi(opcion);
        if (op == 1)
        {
            estado_fuga = 1;
            printf("[ENGINE] >>> FUGA ACTIVADA. El Monitor lo detectara en el siguiente latido <<<\n");
        }
        else if (op == 2)
        {
            estado_fuga = 0;
            printf("[ENGINE] >>> FUGA REPARADA. Enviando OK al Monitor <<<\n");
        }
        else if (op == 3)
        {
            lanzar_riego_async();
        }
        else if (op == 4)
        {
            abortar_riego = 1;
            printf("[ENGINE] >>> Parada manual de riego solicitada <<<\n");
        }
    }

    return 0;
}