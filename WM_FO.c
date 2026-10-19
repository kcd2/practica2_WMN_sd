// Archivo: WM_FO.c
// Compilación: gcc -o WM_FO WM_FO.c -lrdkafka

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

#define TOPIC_PETICIONES "wm_peticiones"
#define TOPIC_RESPUESTAS "wm_respuestas_fo"

// Bloquea hasta recibir respuesta de Central; si es AUTORIZADO, espera también a FIN_RIEGO
void esperar_respuesta(rd_kafka_t *rk_consumer, const char *id_operador_actual, const char *id_estacion_esperada) {
    printf("Esperando confirmacion de la Central para %s...\n", id_estacion_esperada);
    int riego_en_curso = 0;

    while (1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);

        if (rkmessage) {
            if (!rkmessage->err && rkmessage->payload) {
                char payload[256];
                snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
                payload[strcspn(payload, "\r\n")] = '\0';

                char *id_op = strtok(payload, "#");
                char *id_est = strtok(NULL, "#");
                char *resultado = strtok(NULL, "#");

                if (id_op && id_est && resultado && strcmp(id_est, id_estacion_esperada) == 0) {
                    // 1. Respuesta directa a la solicitud de este operario
                    if (!riego_en_curso && strcmp(id_op, id_operador_actual) == 0) {
                        if (strcmp(resultado, "AUTORIZADO") == 0) {
                            printf(">>> [EXITO] Riego AUTORIZADO en la estacion %s. Luz VERDE. Esperando fin del servicio...\n", id_est);
                            riego_en_curso = 1; // Nos quedamos en el bucle hasta que llegue FIN_RIEGO
                        } else if (strcmp(resultado, "DENEGADO") == 0) {
                            printf(">>> [DENEGADO] La estacion %s esta ocupada, bloqueada o con averia.\n\n", id_est);
                            rd_kafka_message_destroy(rkmessage);
                            break;
                        } else {
                            printf(">>> [ERROR] La Central reporta: %s\n\n", resultado);
                            rd_kafka_message_destroy(rkmessage);
                            break;
                        }
                    }
                    // 2. Resumen de finalización del riego activo
                    else if (riego_en_curso && (strcmp(id_op, id_operador_actual) == 0 || strcmp(id_op, "TODOS") == 0) &&
                             strncmp(resultado, "FIN_RIEGO", 9) == 0) {
                        printf(">>> [SERVICIO FINALIZADO en %s] Resumen: %s\n\n", id_est, resultado);
                        rd_kafka_message_destroy(rkmessage);
                        break; // Ahora sí terminó el riego: salimos para cumplir los 4s de espera
                    }
                }
            }
            rd_kafka_message_destroy(rkmessage);
        }
    }
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    sleep(20); // Espera a que Central (10s) y Monitor hayan registrado las estaciones

    if (argc < 3) {
        fprintf(stderr, "Uso: %s <IP_KAFKA:PUERTO> <ID_OPERADOR> [FICHERO_RUTINAS]\n", argv[0]);
        return 1;
    }

    char *brokers = argv[1];
    char *id_operador = argv[2];
    char *fichero = (argc == 4) ? argv[3] : NULL;
    char errstr[512];

    printf("--- Iniciando Terminal de Operario: %s ---\n", id_operador);
    printf("Conectando al Broker Kafka: %s\n\n", brokers);

    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", brokers, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    rd_kafka_topic_t *topic_peticiones = rd_kafka_topic_new(rk_producer, TOPIC_PETICIONES, NULL);

    rd_kafka_conf_t *conf_cons = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_cons, "bootstrap.servers", brokers, errstr, sizeof(errstr));

    char group_id[64];
    snprintf(group_id, sizeof(group_id), "grupo_fo_%s", id_operador);
    rd_kafka_conf_set(conf_cons, "group.id", group_id, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "auto.offset.reset", "latest", errstr, sizeof(errstr));

    rd_kafka_t *rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf_cons, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(rk_consumer);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, TOPIC_RESPUESTAS, RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk_consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (fichero != NULL) {
        printf("[MODO AUTOMATICO] Leyendo fichero: %s\n", fichero);
        FILE *file = fopen(fichero, "r");
        if (!file) {
            perror("Error al abrir el fichero de rutinas");
            return 1;
        }

        char id_estacion[128];
        while (fgets(id_estacion, sizeof(id_estacion), file)) {
            id_estacion[strcspn(id_estacion, "\r\n")] = '\0';
            if (strlen(id_estacion) == 0) continue;

            char payload[256];
            snprintf(payload, sizeof(payload), "%s#%s", id_operador, id_estacion);

            printf(">> Solicitando riego para: %s...\n", id_estacion);

            rd_kafka_produce(topic_peticiones, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                             payload, strlen(payload), NULL, 0, NULL);
            rd_kafka_flush(rk_producer, 1000);

            esperar_respuesta(rk_consumer, id_operador, id_estacion);

            printf("[FO] Esperando 4 segundos antes de la siguiente peticion...\n\n");
            sleep(4);
        }
        fclose(file);
        printf("[MODO AUTOMATICO] Fichero procesado al 100%%.\n");
    } else {
        printf("[MODO MANUAL] Escribe el ID de la estacion para solicitar riego (o 'salir'):\n");
        char entrada[128];
        while (1) {
            printf("\nFO-%s > ", id_operador);
            if (!fgets(entrada, sizeof(entrada), stdin)) break;

            entrada[strcspn(entrada, "\r\n")] = '\0';
            if (strcmp(entrada, "salir") == 0) break;
            if (strlen(entrada) == 0) continue;

            char payload[256];
            snprintf(payload, sizeof(payload), "%s#%s", id_operador, entrada);

            rd_kafka_produce(topic_peticiones, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                             payload, strlen(payload), NULL, 0, NULL);
            rd_kafka_flush(rk_producer, 1000);

            esperar_respuesta(rk_consumer, id_operador, entrada);
            sleep(4);
        }
    }

    rd_kafka_topic_destroy(topic_peticiones);
    rd_kafka_destroy(rk_producer);
    rd_kafka_consumer_close(rk_consumer);
    rd_kafka_destroy(rk_consumer);
    return 0;
}