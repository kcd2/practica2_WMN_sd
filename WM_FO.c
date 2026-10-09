#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

#define TOPIC_PETICIONES "wm_peticiones"
#define TOPIC_RESPUESTAS "wm_respuestas_fo"

// Función para bloquear y esperar la respuesta de la Central sobre una estación concreta
void esperar_respuesta(rd_kafka_t *rk_consumer, const char *id_estacion_esperada) {
    printf("Esperando confirmacion de la Central...\n");
    
    while (1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);
        
        if (rkmessage) {
            if (!rkmessage->err) {
                char payload[256];
                snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
                
                // El formato que nos envía la Central es: ID_OPERADOR#ID_ESTACION#RESULTADO
                char *id_op = strtok(payload, "#");
                char *id_est = strtok(NULL, "#");
                char *resultado = strtok(NULL, "#");

                // Solo procesamos la respuesta si es para la estación que acabamos de solicitar
                if (id_est && strcmp(id_est, id_estacion_esperada) == 0) {
                    if (resultado) {
                        if (strcmp(resultado, "AUTORIZADO") == 0) {
                            printf(">>> [EXITO] Riego AUTORIZADO en la estacion %s. Luz VERDE.\n\n", id_est);
                        } else if (strcmp(resultado, "DENEGADO") == 0) {
                            printf(">>> [DENEGADO] La estacion %s esta ocupada o con averia.\n\n", id_est);
                        } else {
                            printf(">>> [ERROR] La Central reporta: %s\n\n", resultado);
                        }
                    }
                    rd_kafka_message_destroy(rkmessage);
                    break; // Salimos del bucle al recibir nuestra respuesta
                }
            }
            rd_kafka_message_destroy(rkmessage);
        }
    }
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    sleep(15);
    
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

    // Configuración del PRODUCTOR (Para enviar peticiones)
    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", brokers, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    rd_kafka_topic_t *topic_peticiones = rd_kafka_topic_new(rk_producer, TOPIC_PETICIONES, NULL);

    // Configuración del CONSUMIDOR (Para escuchar respuestas)
    rd_kafka_conf_t *conf_cons = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_cons, "bootstrap.servers", brokers, errstr, sizeof(errstr));
    
    // Asignamos un group.id único a cada operario para que todos reciban sus propios mensajes
    char group_id[64];
    snprintf(group_id, sizeof(group_id), "grupo_fo_%s", id_operador);
    rd_kafka_conf_set(conf_cons, "group.id", group_id, errstr, sizeof(errstr));
    
    rd_kafka_t *rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf_cons, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(rk_consumer);
    
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, TOPIC_RESPUESTAS, RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk_consumer, topics);

    // Lógica de lectura automatizada (Fichero)
    if (fichero != NULL) {
        printf("[MODO AUTOMATICO] Leyendo fichero: %s\n", fichero);
        FILE *file = fopen(fichero, "r");
        if (!file) {
            perror("Error al abrir el fichero de rutinas");
            return 1;
        }

        char id_estacion[128];
        while (fgets(id_estacion, sizeof(id_estacion), file)) {
            id_estacion[strcspn(id_estacion, "\r\n")] = '\0'; // Limpiar saltos de línea
            if (strlen(id_estacion) == 0) continue;
            
            // Construir trama: ID_OPERADOR#ID_ESTACION
            char payload[256];
            snprintf(payload, sizeof(payload), "%s#%s", id_operador, id_estacion);
            
            printf(">> Solicitando riego para: %s...\n", id_estacion);
            
            // Enviar petición
            rd_kafka_produce(topic_peticiones, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                             payload, strlen(payload), NULL, 0, NULL);
            rd_kafka_flush(rk_producer, 1000);

            // Escuchar respuesta y bloquear hasta que llegue
            esperar_respuesta(rk_consumer, id_estacion);
            
            // Pausa obligatoria entre peticiones
            sleep(4); 
        }
        fclose(file);
        printf("[MODO AUTOMATICO] Fichero procesado al 100%%.\n");
    } else {
        // Lógica de lectura interactiva (Teclado)
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

            esperar_respuesta(rk_consumer, entrada);
        }
    }

    // Limpieza de memoria
    rd_kafka_topic_destroy(topic_peticiones);
    rd_kafka_destroy(rk_producer);
    rd_kafka_consumer_close(rk_consumer);
    rd_kafka_destroy(rk_consumer);
    return 0;
}