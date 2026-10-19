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
#include <sys/stat.h>
#include <librdkafka/rdkafka.h>

char *kafka_broker;
char *monitor_ip;
char *monitor_puerto;
char *id_estacion;

int s_monitor = -1;
volatile int monitor_conectado = 0;
volatile int central_disponible = 0;
volatile int estado_fuga = 0;   // 0 = OK, 1 = FUGA (KO)
volatile int abortar_riego = 0; // 1 = Parada ordenada por Central o manual (sin ser fuga)
volatile int regando = 0;
volatile int fuera_servicio = 0;
volatile int ejecutando = 1;
volatile int error_entrega_engine = 0;
char ruta_outbox_engine[256];
char ruta_estado_engine[256];
pthread_mutex_t mutex_outbox_engine = PTHREAD_MUTEX_INITIALIZER;

int guardar_estado_riego(const char *estado, double volumen, int segundos) {
    char temporal[320];
    snprintf(temporal, sizeof(temporal), "%s.tmp", ruta_estado_engine);
    FILE *file = fopen(temporal, "w");
    if (file == NULL) return -1;
    int escrito = fprintf(file, "%s#%.2f#%d\n", estado, volumen, segundos) > 0;
    if (escrito && fflush(file) != 0) escrito = 0;
    if (escrito && fsync(fileno(file)) != 0) escrito = 0;
    if (fclose(file) != 0) escrito = 0;
    if (!escrito || rename(temporal, ruta_estado_engine) != 0) {
        remove(temporal);
        return -1;
    }
    return 0;
}

int guardar_evento_engine(const char *payload) {
    FILE *file = fopen(ruta_outbox_engine, "a");
    if (file == NULL) return -1;
    int escrito = fprintf(file, "%s\n", payload) > 0;
    if (escrito && fflush(file) != 0) escrito = 0;
    if (escrito && fsync(fileno(file)) != 0) escrito = 0;
    if (fclose(file) != 0) escrito = 0;
    if (escrito) return 0;
    return -1;
}

int leer_evento_engine(char *payload, size_t tam) {
    FILE *file = fopen(ruta_outbox_engine, "r");
    if (file == NULL) return 0;
    int leido = fgets(payload, (int)tam, file) != NULL;
    if (leido) payload[strcspn(payload, "\r\n")] = '\0';
    fclose(file);
    return leido && payload[0] != '\0';
}

int quitar_evento_engine(void) {
    char temporal[320];
    char linea[512];
    snprintf(temporal, sizeof(temporal), "%s.tmp", ruta_outbox_engine);
    FILE *origen = fopen(ruta_outbox_engine, "r");
    if (origen == NULL) return -1;
    FILE *destino = fopen(temporal, "w");
    if (destino == NULL) {
        fclose(origen);
        return -1;
    }
    int resultado = 0;
    if (fgets(linea, sizeof(linea), origen) == NULL) resultado = -1;
    while (resultado == 0 && fgets(linea, sizeof(linea), origen) != NULL) {
        if (fputs(linea, destino) == EOF) resultado = -1;
    }
    if (fflush(destino) != 0 || fsync(fileno(destino)) != 0) resultado = -1;
    if (fclose(origen) != 0) resultado = -1;
    if (fclose(destino) != 0) resultado = -1;
    if (resultado == 0 && rename(temporal, ruta_outbox_engine) != 0) resultado = -1;
    if (resultado != 0) remove(temporal);
    return resultado;
}

void confirmar_entrega_engine(rd_kafka_t *rk, const rd_kafka_message_t *mensaje, void *opaque) {
    (void)rk;
    (void)opaque;
    if (mensaje->err != RD_KAFKA_RESP_ERR_NO_ERROR) error_entrega_engine = 1;
}

void *hilo_publicador_engine(void *arg) {
    (void)arg;
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", kafka_broker, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));
    rd_kafka_conf_set_dr_msg_cb(conf, confirmar_entrega_engine);
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (producer == NULL) {
        fprintf(stderr, "[ENGINE] No se pudo crear el productor persistente: %s\n", errstr);
        return NULL;
    }
    rd_kafka_topic_t *topic = rd_kafka_topic_new(producer, "wm_peticiones", NULL);
    if (topic == NULL) {
        fprintf(stderr, "[ENGINE] No se pudo abrir el topic de eventos: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(producer);
        return NULL;
    }
    while (ejecutando) {
        char payload[512];
        int encontrado = 0;
        pthread_mutex_lock(&mutex_outbox_engine);
        encontrado = leer_evento_engine(payload, sizeof(payload));
        pthread_mutex_unlock(&mutex_outbox_engine);
        if (!encontrado) {
            sleep(1);
            continue;
        }
        error_entrega_engine = 0;
        int producido = rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                         payload, strlen(payload), NULL, 0, NULL);
        int vaciado = RD_KAFKA_RESP_ERR__FAIL;
        if (producido == 0) vaciado = rd_kafka_flush(producer, 5000);
        if (producido == 0 && vaciado == RD_KAFKA_RESP_ERR_NO_ERROR && !error_entrega_engine) {
            pthread_mutex_lock(&mutex_outbox_engine);
            if (quitar_evento_engine() == 0 && strncmp(payload, "FIN_RIEGO#", 9) == 0) remove(ruta_estado_engine);
            pthread_mutex_unlock(&mutex_outbox_engine);
        } else {
            fprintf(stderr, "[ENGINE] Evento pendiente de Kafka; permanece guardado para reintento.\n");
            rd_kafka_poll(producer, 0);
            sleep(1);
        }
    }
    rd_kafka_flush(producer, 5000);
    rd_kafka_topic_destroy(topic);
    rd_kafka_destroy(producer);
    return NULL;
}

int recuperar_riego_engine(void) {
    FILE *file = fopen(ruta_estado_engine, "r");
    if (file == NULL) return 0;
    char estado[32] = "";
    double volumen = 0.0;
    int segundos = 0;
    int valido = fscanf(file, "%31[^#]#%lf#%d", estado, &volumen, &segundos) == 3;
    fclose(file);
    if (!valido) return -1;
    if (strcmp(estado, "FIN_PENDING") == 0) {
        char evento_pendiente[512];
        if (!leer_evento_engine(evento_pendiente, sizeof(evento_pendiente))) remove(ruta_estado_engine);
        return 0;
    }
    if (strcmp(estado, "ACTIVE") != 0) return -1;
    char payload[256];
    snprintf(payload, sizeof(payload), "FIN_RIEGO#%s#%.2f#%d#ENGINE_DESCONECTADO", id_estacion, volumen, segundos);
    pthread_mutex_lock(&mutex_outbox_engine);
    int guardado = guardar_evento_engine(payload) == 0;
    if (guardado && guardar_estado_riego("FIN_PENDING", volumen, segundos) != 0) guardado = 0;
    pthread_mutex_unlock(&mutex_outbox_engine);
    if (guardado) return 1;
    return -1;
}

int consultar_estado_riego(char *estado, size_t tam) {
    FILE *file = fopen(ruta_estado_engine, "r");
    if (file == NULL) return 0;
    int leido = fgets(estado, (int)tam, file) != NULL;
    fclose(file);
    if (!leido) return 0;
    estado[strcspn(estado, "#\r\n")] = '\0';
    return 1;
}

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
        monitor_conectado = 1;

        while (ejecutando)
        {
            n = read(s_monitor, buffer, sizeof(buffer) - 1);
            if (n <= 0)
            {
                printf("[ENGINE] Conexion perdida con el Monitor local. Reconectando...\n");
                monitor_conectado = 0;
                close(s_monitor);
                s_monitor = -1;
                break;
            }

            buffer[n] = '\0';
            if (strstr(buffer, "CENTRAL_KO") != NULL) {
                central_disponible = 0;
            }
            if (strstr(buffer, "CENTRAL_OK") != NULL) {
                central_disponible = 1;
            }
            if (strstr(buffer, "DETENER_FUGA") != NULL)
            {
                estado_fuga = 1;
            }
            else if (strstr(buffer, "RESTABLECER") != NULL)
            {
                estado_fuga = 0;
            }

            const char *latido = "OK\n";
            if (estado_fuga) {
                latido = "KO\n";
            }
            enviados = write(s_monitor, latido, strlen(latido));
            if (enviados <= 0)
            {
                printf("[ENGINE] Error al enviar estado al Monitor local. Reconectando...\n");
                monitor_conectado = 0;
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
    int es_manual = 0;
    if (arg != NULL) {
        es_manual = *((int *)arg);
        free(arg);
    }
    if (!monitor_conectado)
    {
        printf("[ENGINE] Orden de riego rechazada: el Monitor no está conectado.\n");
        return NULL;
    }
    if (!central_disponible)
    {
        printf("[ENGINE] Orden de riego rechazada: WM_Central no est\xE1 disponible.\n");
        return NULL;
    }
    if (fuera_servicio)
    {
        printf("[ENGINE] Orden de riego rechazada: la estación está fuera de servicio.\n");
        return NULL;
    }
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

    pthread_mutex_lock(&mutex_outbox_engine);
    int estado_guardado = guardar_estado_riego("ACTIVE", volumen_total, segundos) == 0;
    pthread_mutex_unlock(&mutex_outbox_engine);
    if (!estado_guardado) {
        regando = 0;
        fprintf(stderr, "[ENGINE] No se pudo guardar el estado del riego; se cancela el inicio.\n");
        return NULL;
    }

    if (es_manual) {
        char payload_inicio[256];
        snprintf(payload_inicio, sizeof(payload_inicio), "%s#INICIO_RIEGO#MANUAL_WS", id_estacion);
        pthread_mutex_lock(&mutex_outbox_engine);
        int evento_guardado = guardar_evento_engine(payload_inicio) == 0;
        pthread_mutex_unlock(&mutex_outbox_engine);
        if (!evento_guardado) abortar_riego = 1;
    }

    printf("\n======================================================\n");
    printf("[ENGINE %s] >>> ABRIENDO ELECTROVÁLVULA - INICIANDO RIEGO <<<\n", id_estacion);
    printf("======================================================\n");

    while (segundos < 10 && !estado_fuga && !abortar_riego && ejecutando && monitor_conectado)
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
        pthread_mutex_lock(&mutex_outbox_engine);
        int telemetria_guardada = guardar_evento_engine(payload_tel) == 0;
        int estado_actualizado = guardar_estado_riego("ACTIVE", volumen_total, segundos) == 0;
        pthread_mutex_unlock(&mutex_outbox_engine);
        if (!telemetria_guardada || !estado_actualizado) {
            fprintf(stderr, "[ENGINE] No se pudo guardar la telemetría de forma duradera; se detiene el riego.\n");
            abortar_riego = 1;
            break;
        }
    }

    if (!monitor_conectado) abortar_riego = 1;

    char payload_fin[256];
    const char *causa_fin = "NORMAL";
    if (estado_fuga) {
        causa_fin = "FUGA";
    } else if (!monitor_conectado) {
        causa_fin = "ENGINE_DESCONECTADO";
    } else if (abortar_riego) {
        causa_fin = "DETENIDO";
    }
    snprintf(payload_fin, sizeof(payload_fin), "FIN_RIEGO#%s#%.2f#%d#%s", id_estacion, volumen_total, segundos, causa_fin);
    pthread_mutex_lock(&mutex_outbox_engine);
    int fin_guardado = guardar_evento_engine(payload_fin) == 0;
    if (fin_guardado && guardar_estado_riego("FIN_PENDING", volumen_total, segundos) != 0) {
        fprintf(stderr, "[ENGINE] El resumen está en cola, pero no se pudo actualizar el diario local.\n");
    }
    pthread_mutex_unlock(&mutex_outbox_engine);
    if (!fin_guardado) {
        fprintf(stderr, "[ENGINE] No se pudo guardar el resumen final en la cola local.\n");
    }

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

    if (fin_guardado) {
        while (ejecutando && access(ruta_estado_engine, F_OK) == 0) sleep(1);
    }
    regando = 0;

    return NULL;
}

void lanzar_riego_async(int manual)
{
    if (access(ruta_estado_engine, F_OK) == 0 && !regando)
    {
        printf("[ENGINE] Hay un resumen pendiente de confirmar en Kafka; no se inicia otro riego.\n");
        return;
    }
    if (!monitor_conectado)
    {
        printf("[ENGINE] No se puede iniciar el riego: el Monitor no está conectado.\n");
        return;
    }
    if (!central_disponible)
    {
        printf("[ENGINE] No se puede iniciar el riego: WM_Central no est\xE1 disponible.\n");
        return;
    }
    int *parametro_manual = malloc(sizeof(int));
    if (parametro_manual == NULL) return;
    *parametro_manual = manual;
    pthread_t th_riego;
    if (pthread_create(&th_riego, NULL, hilo_ciclo_riego, parametro_manual) != 0) {
        free(parametro_manual);
        return;
    }
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
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

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
        int evento_procesado = 1;

        if (!msg->err && msg->payload)
        {
            char payload[512];
            snprintf(payload, sizeof(payload), "%.*s", (int)msg->len, (char *)msg->payload);

            if (strstr(payload, id_estacion) != NULL)
            {
                if (strstr(payload, "INICIAR_RIEGO") != NULL)
                {
                    printf("[ENGINE] Orden de Kafka recibida: %s\n", payload);
                    if (!regando) {
                        int riego_guardado = 0;
                        while (ejecutando && !riego_guardado) {
                            char estado_guardado[32];
                            if (consultar_estado_riego(estado_guardado, sizeof(estado_guardado)) && strcmp(estado_guardado, "ACTIVE") == 0) {
                                riego_guardado = 1;
                                break;
                            }
                            if (!regando) lanzar_riego_async(0);
                            sleep(1);
                        }
                        if (!riego_guardado) evento_procesado = 0;
                    }
                }
                if (strstr(payload, "FUERA_SERVICIO") != NULL)
                {
                    fuera_servicio = 1;
                    abortar_riego = 1;
                    printf("[ENGINE] Estación marcada FUERA_SERVICIO por Central.\n");
                }
                if (strstr(payload, "ACTIVAR_ESTACION") != NULL)
                {
                    fuera_servicio = 0;
                    printf("[ENGINE] Estación activada por Central.\n");
                }
                if (strstr(payload, "DETENER_RIEGO") != NULL)
                {
                    printf("[ENGINE] ¡Orden de DETENER_RIEGO recibida desde Central! Deteniendo válvula...\n");
                    abortar_riego = 1;
                }
            }
        }
        if (!msg->err && evento_procesado) {
            rd_kafka_resp_err_t error_commit = rd_kafka_commit_message(rk, msg, 0);
            if (error_commit != RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "[ENGINE] No se pudo confirmar la orden Kafka; se podrá recuperar en el siguiente arranque.\n");
            }
        } else if (!msg->err && !evento_procesado) {
            fprintf(stderr, "[ENGINE] Orden no confirmada; se conserva sin avanzar el offset.\n");
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

    mkdir("engine_data", 0777);
    snprintf(ruta_outbox_engine, sizeof(ruta_outbox_engine), "engine_data/%s_outbox.txt", id_estacion);
    snprintf(ruta_estado_engine, sizeof(ruta_estado_engine), "engine_data/%s_riego.txt", id_estacion);
    if (recuperar_riego_engine() < 0) {
        fprintf(stderr, "[ENGINE] No se pudo recuperar el diario del riego anterior.\n");
        return 1;
    }

    pthread_t th_publicador;
    if (pthread_create(&th_publicador, NULL, hilo_publicador_engine, NULL) != 0) {
        fprintf(stderr, "[ENGINE] No se pudo iniciar el publicador persistente de Kafka.\n");
        return 1;
    }

    pthread_t th_mon;
    pthread_create(&th_mon, NULL, hilo_conexion_monitor, NULL);

    pthread_t th_kafka;
    pthread_create(&th_kafka, NULL, hilo_consumidor_kafka, NULL);

    char opcion[16];
    while (ejecutando)
    {
        printf("\n--- MENU ENGINE (%s) ---\n", id_estacion);
        const char *salud_fisica = "OK";
        const char *estado_estacion = "ACTIVA";
        const char *estado_valvula = "CERRADA";
        if (estado_fuga) {
            salud_fisica = "CON FUGA (KO)";
        }
        if (fuera_servicio) {
            estado_estacion = "FUERA_SERVICIO";
        }
        if (regando) {
            estado_valvula = "REGANDO";
        }
        printf("Salud fisica: %s | Estacion: %s | Valvula: %s\n", salud_fisica, estado_estacion, estado_valvula);
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
            lanzar_riego_async(1);
        }
        else if (op == 4)
        {
            abortar_riego = 1;
            printf("[ENGINE] >>> Parada manual de riego solicitada <<<\n");
        }
    }

    return 0;
}
