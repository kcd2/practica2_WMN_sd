
// Compilación: gcc WM_WS_E.c -o WM_WS_E -lpthread -lrdkafka

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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
volatile int estado_fuga = 0; // 0 = OK, 1 = FUGA (KO)
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
 * HILO CLIENTE LOCAL PARA ENVIAR LATIDOS DE SALUD AL MONITOR CADA SEGUNDO
 * ========================================================================= */
void *hilo_conexion_monitor(void *arg)
{
    struct sockaddr_in direccion;
    int n;
    char buffer[128];

    while (ejecutando)
    {
        s_monitor = socket(AF_INET, SOCK_STREAM, 0);
        if (s_monitor == -1)
        {
            sleep(2);
            continue;
        }

        direccion.sin_family = AF_INET;
        direccion.sin_addr.s_addr = inet_addr(monitor_ip);
        direccion.sin_port = htons(atoi(monitor_puerto));

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
            // Enviar latido de salud según el estado físico actual
            const char *latido = estado_fuga ? "FUGA\n" : "OK\n";
            int enviados = write(s_monitor, latido, strlen(latido));
            if (enviados <= 0)
            {
                printf("[ENGINE] Conexion perdida con el Monitor local. Reconectando...\n");
                close(s_monitor);
                s_monitor = -1;
                break;
            }

            // Leer si el Monitor nos ordena algo (ej. DETENER_FUGA / RESTABLECER)
            n = read(s_monitor, buffer, sizeof(buffer) - 1);
            if (n > 0)
            {
                buffer[n] = '\0';
                if (strstr(buffer, "DETENER_FUGA") != NULL) estado_fuga = 1;
                else if (strstr(buffer, "RESTABLECER") != NULL) estado_fuga = 0;
            }

            sleep(1);
        }
    }
    return NULL;
}

/* =========================================================================
 * SIMULACIÓN DEL CICLO DE RIEGO Y CAUDAL
 * ========================================================================= */
void ejecutar_ciclo_riego()
{
    if (estado_fuga)
    {
        printf("[ENGINE] Orden de riego rechazada: La estación tiene una FUGA activa.\n");
        return;
    }

    regando = 1;
    double volumen_total = 0.0;
    int segundos = 0;

    printf("\n======================================================\n");
    printf("[ENGINE %s] >>> ABRIENDO ELECTROVÁLVULA - INICIANDO RIEGO <<<\n", id_estacion);
    printf("======================================================\n");

    while (segundos < 10 && !estado_fuga && ejecutando)
    {
        sleep(1);
        segundos++;
        double caudal = 5.5 + ((rand() % 10) - 5) * 0.1;
        volumen_total += caudal;
        printf("[ENGINE %s] Regando... %02ds/10s | Caudal: %.2f L/s | Volumen: %.2f L\n",
               id_estacion, segundos, caudal, volumen_total);
    }

    regando = 0;
    if (estado_fuga)
    {
        printf("[ENGINE %s] Riego abortado por fuga. Volumen suministrado: %.2f L\n", id_estacion, volumen_total);
    }
    else
    {
        printf("[ENGINE %s] Riego finalizado correctamente. Volumen total: %.2f L\n", id_estacion, volumen_total);
    }
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

            // Si la orden va dirigida a esta estación
            if (strstr(payload, id_estacion) != NULL && strstr(payload, "INICIAR_RIEGO") != NULL)
            {
                printf("[ENGINE] Orden de Kafka recibida: %s\n", payload);
                ejecutar_ciclo_riego();
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

    // 1. Lanzar hilo para enviar latidos al Monitor local por sockets TCP
    pthread_t th_mon;
    pthread_create(&th_mon, NULL, hilo_conexion_monitor, NULL);

    // 2. Lanzar hilo consumidor de Kafka para recibir órdenes de riego
    pthread_t th_kafka;
    pthread_create(&th_kafka, NULL, hilo_consumidor_kafka, NULL);

    // 3. Menú interactivo de pruebas en el Engine (para provocar fuga con una tecla)
    char opcion[16];
    while (ejecutando)
    {
        printf("\n--- MENU ENGINE (%s) ---\n", id_estacion);
        printf("Salud fisica: %s | Valvula: %s\n", estado_fuga ? "CON FUGA (KO)" : "OK", regando ? "REGANDO" : "CERRADA");
        printf("1. Provocar FUGA de agua (envia KO/FUGA al Monitor)\n");
        printf("2. Reparar FUGA (envia OK al Monitor)\n");
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
    }

    return 0;
}