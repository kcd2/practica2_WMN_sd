// Archivo: WM_FO.c
// Compilación: gcc -o WM_FO WM_FO.c -lrdkafka

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <librdkafka/rdkafka.h>

#define TOPIC_PETICIONES "wm_peticiones"
#define TOPIC_RESPUESTAS "wm_respuestas_fo"

int linea_checkpoint_actual = -1;
int error_entrega_fo = 0;

/* Guarda el punto de reanudación del operario antes de enviar o esperar una petición. */
int guardar_checkpoint(const char *id_estacion, int linea, const char *estado) {
    mkdir("fo_data", 0777);
    FILE *file = fopen("fo_data/operacion_pendiente.txt", "w");
    if (file == NULL) {
        return -1;
    }
    int escrito = fprintf(file, "%s\n%d\n%s\n", id_estacion, linea, estado) > 0;
    if (escrito && fflush(file) != 0) {
        escrito = 0;
    }
    if (fclose(file) != 0) {
        escrito = 0;
    }
    if (escrito) {
        return 0;
    }
    return -1;
}

// Lee la operación guardada para poder reanudarla.
int leer_checkpoint(char *id_estacion, size_t tam, int *linea, char *estado, size_t tam_estado) {
    FILE *file = fopen("fo_data/operacion_pendiente.txt", "r");
    if (file == NULL) {
        return 0;
    }
    char numero_linea[32];
    int leido = fgets(id_estacion, (int)tam, file) != NULL;
    if (leido) {
        id_estacion[strcspn(id_estacion, "\r\n")] = '\0';
        leido = fgets(numero_linea, sizeof(numero_linea), file) != NULL;
    }
    if (leido) {
        numero_linea[strcspn(numero_linea, "\r\n")] = '\0';
        if (sscanf(numero_linea, "%d", linea) != 1) {
            *linea = -1;
        }
        snprintf(estado, tam_estado, "WAITING");
        if (fgets(estado, (int)tam_estado, file) != NULL) {
            estado[strcspn(estado, "\r\n")] = '\0';
        }
        if (id_estacion[0] == '\0') {
            leido = 0;
        }
    }
    fclose(file);
    return leido;
}

// Borra el checkpoint cuando ya no queda una operación pendiente.
void limpiar_checkpoint(void) {
    remove("fo_data/operacion_pendiente.txt");
}

// Lee el último resultado guardado del operario.
int leer_checkpoint_completado(char *topic, size_t tam_topic, int *particion, long long *offset, char *resultado, size_t tam_resultado) {
    FILE *file = fopen("fo_data/operacion_pendiente.txt", "r");
    if (file == NULL) {
        return 0;
    }
    char linea[1024];
    int completado = 0;
    if (fgets(linea, sizeof(linea), file) != NULL &&
        fgets(linea, sizeof(linea), file) != NULL &&
        fgets(linea, sizeof(linea), file) != NULL) {
        linea[strcspn(linea, "\r\n")] = '\0';
        if (strcmp(linea, "DONE") == 0 &&
            fgets(topic, (int)tam_topic, file) != NULL &&
            fgets(linea, sizeof(linea), file) != NULL) {
            *particion = atoi(linea);
            if (fgets(linea, sizeof(linea), file) != NULL) {
                *offset = atoll(linea);
                if (fgets(resultado, (int)tam_resultado, file) != NULL) {
                    topic[strcspn(topic, "\r\n")] = '\0';
                    resultado[strcspn(resultado, "\r\n")] = '\0';
                    completado = topic[0] != '\0' && resultado[0] != '\0';
                }
            }
        }
    }
    fclose(file);
    return completado;
}

// Confirma a Kafka los mensajes ya guardados en el checkpoint.
int confirmar_offset_guardado(rd_kafka_t *rk_consumer, const char *topic, int particion, long long offset) {
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(1);
    if (offsets == NULL) {
        return -1;
    }
    rd_kafka_topic_partition_t *elemento = rd_kafka_topic_partition_list_add(offsets, topic, particion);
    if (elemento == NULL) {
        rd_kafka_topic_partition_list_destroy(offsets);
        return -1;
    }
    elemento->offset = offset + 1;
    rd_kafka_resp_err_t error = rd_kafka_commit(rk_consumer, offsets, 0);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
        return 0;
    }
    return -1;
}

// Guarda el resultado recibido y su posición en Kafka.
int guardar_resultado_checkpoint(const char *id_estacion, int linea, rd_kafka_message_t *mensaje, const char *resultado) {
    const char *topic = rd_kafka_topic_name(mensaje->rkt);
    FILE *file = fopen("fo_data/operacion_pendiente.txt", "w");
    if (file == NULL || topic == NULL) {
        if (file != NULL) {
            fclose(file);
        }
        return -1;
    }
    int escrito = fprintf(file, "%s\n%d\nDONE\n%s\n%d\n%lld\n%s\n",
                          id_estacion, linea, topic, mensaje->partition,
                          (long long)mensaje->offset, resultado) > 0;
    if (escrito && fflush(file) != 0) {
        escrito = 0;
    }
    if (fclose(file) != 0) {
        escrito = 0;
    }
    if (escrito) {
        return 0;
    }
    return -1;
}

// Confirma a Kafka que el mensaje ya se procesó.
int confirmar_mensaje(rd_kafka_t *rk_consumer, rd_kafka_message_t *mensaje) {
    rd_kafka_resp_err_t error = rd_kafka_commit_message(rk_consumer, mensaje, 0);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "[FO] No se pudo confirmar el offset Kafka; el mensaje se volverá a leer.\n");
        return -1;
    }
    return 0;
}

// Anota si Kafka confirmó la entrega del mensaje del operario.
void confirmar_entrega_fo(rd_kafka_t *rk, const rd_kafka_message_t *mensaje, void *opaque) {
    (void)rk;
    (void)opaque;
    if (mensaje->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        error_entrega_fo = 1;
    }
}

// Publica un mensaje en Kafka y reintenta si falla el envío.
int publicar_mensaje(rd_kafka_t *rk_producer, rd_kafka_topic_t *topic, const char *payload) {
    int intentos = 0;
    int en_cola = 0;
    while (intentos < 5) {
        if (!en_cola) {
            error_entrega_fo = 0;
            int producido = rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                             (void *)payload, strlen(payload), NULL, 0, NULL);
            if (producido == 0) {
                en_cola = 1;
            }
        }
        if (en_cola) {
            rd_kafka_resp_err_t vaciado = rd_kafka_flush(rk_producer, 5000);
            if (vaciado == RD_KAFKA_RESP_ERR_NO_ERROR && !error_entrega_fo) {
                return 0;
            }
            if (vaciado == RD_KAFKA_RESP_ERR_NO_ERROR && error_entrega_fo) {
                en_cola = 0;
            }
        }
        fprintf(stderr, "[FO] Kafka no confirmó el envío; reintentando (%d/5).\n", intentos + 1);
        rd_kafka_poll(rk_producer, 0);
        intentos++;
        sleep(1);
    }
    return -1;
}

// Guarda el resultado y confirma el mensaje recibido.
int completar_operacion(rd_kafka_t *rk_consumer, const char *id_estacion, rd_kafka_message_t *mensaje, const char *resultado) {
    if (guardar_resultado_checkpoint(id_estacion, linea_checkpoint_actual, mensaje, resultado) != 0) {
        fprintf(stderr, "[FO] No se pudo guardar el resultado; el offset queda pendiente.\n");
        return 0;
    }
    if (confirmar_mensaje(rk_consumer, mensaje) != 0) {
        return 0;
    }
    return 1;
}

// Bloquea hasta recibir respuesta de Central; si es AUTORIZADO, espera también a FIN_RIEGO
int esperar_respuesta(rd_kafka_t *rk_consumer, const char *id_operador_actual, const char *id_estacion_esperada) {
    printf("Esperando confirmacion de la Central para %s...\n", id_estacion_esperada);
    int riego_en_curso = 0;
    time_t inicio_espera = time(NULL);

    while (1) {
        if (time(NULL) - inicio_espera >= 45) {
            printf(">>> [AVISO] La operación en %s sigue pendiente. Se conserva y continúa la espera de WM_Central.\n\n", id_estacion_esperada);
            inicio_espera = time(NULL);
        }
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);

        if (rkmessage) {
            if (!rkmessage->err && rkmessage->payload) {
                char payload[512];
                snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
                payload[strcspn(payload, "\r\n")] = '\0';
                char payload_original[512];
                snprintf(payload_original, sizeof(payload_original), "%s", payload);

                char *id_op = strtok(payload, "#");
                char *id_est = strtok(NULL, "#");
                char *resultado = strtok(NULL, "#");
                char *detalle = strtok(NULL, "#");
                char *valor = strtok(NULL, "#");
                char *duracion = strtok(NULL, "#");

                if (id_op && id_est && resultado && strcmp(id_est, id_estacion_esperada) == 0) {
                    if (strcmp(resultado, "TELEMETRIA") == 0 && strcmp(id_op, id_operador_actual) == 0) {
                        const char *caudal = detalle;
                        const char *volumen = valor;
                        if (caudal == NULL) {
                            caudal = "0";
                        }
                        if (volumen == NULL) {
                            volumen = "0";
                        }
                        printf("[TELEMETRIA %s] Caudal: %s L/min | Volumen acumulado: %s L\n",
                               id_est, caudal, volumen);
                        riego_en_curso = 1;
                    }
                    if (strncmp(resultado, "FIN_RIEGO", 9) == 0 &&
                        (strcmp(id_op, id_operador_actual) == 0 || strcmp(id_op, "TODOS") == 0)) {
                        printf(">>> [SERVICIO FINALIZADO en %s] Resumen: %s\n\n", id_est, resultado);
                        int confirmado = completar_operacion(rk_consumer, id_estacion_esperada, rkmessage, payload_original);
                        rd_kafka_message_destroy(rkmessage);
                        return confirmado;
                    }
                    // 1. Respuesta directa a la solicitud de este operario
                    if (!riego_en_curso && strcmp(id_op, id_operador_actual) == 0) {
                        if (strcmp(resultado, "AUTORIZADO") == 0) {
                            printf(">>> [EXITO] Riego AUTORIZADO en la estacion %s. Luz VERDE. Esperando fin del servicio...\n", id_est);
                            riego_en_curso = 1; // Nos quedamos en el bucle hasta que llegue FIN_RIEGO
                        } else if (strcmp(resultado, "DENEGADO") == 0) {
                            printf(">>> [DENEGADO] La estacion %s esta ocupada, bloqueada o con averia.\n\n", id_est);
                            int confirmado = completar_operacion(rk_consumer, id_estacion_esperada, rkmessage, payload_original);
                            rd_kafka_message_destroy(rkmessage);
                            return confirmado;
                        } else if (strcmp(resultado, "ERROR_AVERIA") == 0) {
                            const char *causa = detalle;
                            const char *volumen = valor;
                            const char *tiempo = duracion;
                            if (causa == NULL) {
                                causa = "AVERIA";
                            }
                            if (volumen == NULL) {
                                volumen = "0";
                            }
                            if (tiempo == NULL) {
                                tiempo = "0";
                            }
                            printf(">>> [AVISO] Riego en %s interrumpido por %s. Parcial: %s L en %s s. Esperando cierre de la operación...\n",
                                   id_est, causa, volumen, tiempo);
                            riego_en_curso = 1;
                        } else if (strcmp(resultado, "ERROR_OPERARIO") == 0) {
                            printf(">>> [DENEGADO] El operario %s no está registrado en WM_Central.\n\n", id_op);
                            int confirmado = completar_operacion(rk_consumer, id_estacion_esperada, rkmessage, payload_original);
                            rd_kafka_message_destroy(rkmessage);
                            return confirmado;
                        } else {
                            printf(">>> [ERROR] La Central reporta: %s\n\n", resultado);
                            int confirmado = completar_operacion(rk_consumer, id_estacion_esperada, rkmessage, payload_original);
                            rd_kafka_message_destroy(rkmessage);
                            return confirmado;
                        }
                    }
                    // 2. Resumen de finalización del riego activo
                    else if (riego_en_curso && (strcmp(id_op, id_operador_actual) == 0 || strcmp(id_op, "TODOS") == 0) &&
                             strcmp(resultado, "ERROR_AVERIA") == 0) {
                        const char *causa = detalle;
                        const char *volumen = valor;
                        const char *tiempo = duracion;
                        if (causa == NULL) {
                            causa = "AVERIA";
                        }
                        if (volumen == NULL) {
                            volumen = "0";
                        }
                        if (tiempo == NULL) {
                            tiempo = "0";
                        }
                        printf(">>> [AVISO] Riego en %s interrumpido por %s. Parcial: %s L en %s s. Esperando cierre de la operación...\n",
                               id_est, causa, volumen, tiempo);
                    }
                    else if (riego_en_curso && (strcmp(id_op, id_operador_actual) == 0 || strcmp(id_op, "TODOS") == 0) &&
                             strncmp(resultado, "FIN_RIEGO", 9) == 0) {
                        printf(">>> [SERVICIO FINALIZADO en %s] Resumen: %s\n\n", id_est, resultado);
                        int confirmado = completar_operacion(rk_consumer, id_estacion_esperada, rkmessage, payload_original);
                        rd_kafka_message_destroy(rkmessage);
                        return confirmado;
                    }
                }
            }
            if (!rkmessage->err && confirmar_mensaje(rk_consumer, rkmessage) != 0) {
                printf("[FO] Se conservará la operación pendiente para volver a procesar el mensaje.\n");
            }
            rd_kafka_message_destroy(rkmessage);
        }
    }
    return 0;
}

void solicitar_estaciones_disponibles(rd_kafka_t *rk_producer, rd_kafka_topic_t *topic_peticiones,
                                      rd_kafka_t *rk_consumer, const char *id_operador) {
    char solicitud[128];
    snprintf(solicitud, sizeof(solicitud), "%s#LISTAR", id_operador);
    if (publicar_mensaje(rk_producer, topic_peticiones, solicitud) != 0) {
        fprintf(stderr, "[FO] No se pudo enviar la solicitud de listado a Kafka.\n");
        return;
    }
    printf("\nEstaciones disponibles comunicadas por WM_Central:\n");

    time_t inicio = time(NULL);
    int recibio_fin = 0;
    while (!recibio_fin && time(NULL) - inicio < 8) {
        rd_kafka_message_t *mensaje = rd_kafka_consumer_poll(rk_consumer, 1000);
        if (!mensaje) {
            continue;
        }
        if (!mensaje->err && mensaje->payload) {
            char payload[512];
            snprintf(payload, sizeof(payload), "%.*s", (int)mensaje->len, (char *)mensaje->payload);
            payload[strcspn(payload, "\r\n")] = '\0';
            char *id_op = strtok(payload, "#");
            char *tipo = strtok(NULL, "#");
            char *id = strtok(NULL, "#");
            char *ubicacion = strtok(NULL, "#");
            char *estado = strtok(NULL, "#");
            if (id_op && tipo && strcmp(id_op, id_operador) == 0) {
                if (strcmp(tipo, "LISTA") == 0) {
                    const char *id_texto = "";
                    const char *ubicacion_texto = "";
                    const char *estado_texto = "";
                    if (id != NULL) {
                        id_texto = id;
                    }
                    if (ubicacion != NULL) {
                        ubicacion_texto = ubicacion;
                    }
                    if (estado != NULL) {
                        estado_texto = estado;
                    }
                    printf("- %s | %s | %s\n", id_texto, ubicacion_texto, estado_texto);
                } else if (strcmp(tipo, "LISTA_FIN") == 0) {
                    if (id && atoi(id) == 0) {
                        printf("No hay estaciones DISPONIBLES en este momento.\n");
                    }
                    recibio_fin = 1;
                }
            }
        }
        if (!mensaje->err) {
            confirmar_mensaje(rk_consumer, mensaje);
        }
        rd_kafka_message_destroy(mensaje);
    }
    if (!recibio_fin) {
        printf("No se recibió la lista de WM_Central dentro del tiempo esperado.\n");
    }
    printf("\n");
}

// Inicia la aplicación del operario y atiende sus solicitudes.
int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    sleep(20); // Espera a que Central (10s) y Monitor hayan registrado las estaciones

    if (argc < 3) {
        fprintf(stderr, "Uso: %s <IP_KAFKA:PUERTO> <ID_OPERADOR> [FICHERO_RUTINAS]\n", argv[0]);
        return 1;
    }

    char *brokers = argv[1];
    char *id_operador = argv[2];
    char *fichero = NULL;
    if (argc == 4) {
        fichero = argv[3];
    }
    char errstr[512];

    printf("--- Iniciando Terminal de Operario: %s ---\n", id_operador);
    printf("Conectando al Broker Kafka: %s\n\n", brokers);

    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", brokers, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_prod, "acks", "all", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_prod, "enable.idempotence", "true", errstr, sizeof(errstr));
    rd_kafka_conf_set_dr_msg_cb(conf_prod, confirmar_entrega_fo);
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    if (rk_producer == NULL) {
        fprintf(stderr, "No se pudo crear el productor Kafka: %s\n", errstr);
        return 1;
    }
    rd_kafka_topic_t *topic_peticiones = rd_kafka_topic_new(rk_producer, TOPIC_PETICIONES, NULL);
    if (topic_peticiones == NULL) {
        fprintf(stderr, "No se pudo crear el topic de peticiones: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk_producer);
        return 1;
    }

    rd_kafka_conf_t *conf_cons = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_cons, "bootstrap.servers", brokers, errstr, sizeof(errstr));

    char group_id[64];
    snprintf(group_id, sizeof(group_id), "grupo_fo_%s", id_operador);
    rd_kafka_conf_set(conf_cons, "group.id", group_id, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "auto.offset.reset", "latest", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "enable.auto.commit", "false", errstr, sizeof(errstr));

    rd_kafka_t *rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf_cons, errstr, sizeof(errstr));
    if (rk_consumer == NULL) {
        fprintf(stderr, "No se pudo crear el consumidor Kafka: %s\n", errstr);
        rd_kafka_topic_destroy(topic_peticiones);
        rd_kafka_destroy(rk_producer);
        return 1;
    }
    rd_kafka_poll_set_consumer(rk_consumer);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, TOPIC_RESPUESTAS, RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk_consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    char estacion_pendiente[128];
    int linea_pendiente = -1;
    char estado_checkpoint[32] = "";
    int hay_checkpoint = leer_checkpoint(estacion_pendiente, sizeof(estacion_pendiente), &linea_pendiente,
                                         estado_checkpoint, sizeof(estado_checkpoint));
    char topic_completado[128];
    char resultado_completado[512];
    int particion_completada = -1;
    long long offset_completado = -1;
    if (hay_checkpoint && leer_checkpoint_completado(topic_completado, sizeof(topic_completado),
                                                     &particion_completada, &offset_completado,
                                                     resultado_completado, sizeof(resultado_completado))) {
        printf("[RECUPERACION] Resultado guardado para %s: %s\n", estacion_pendiente, resultado_completado);
        rd_kafka_message_t *mensaje_asignacion = rd_kafka_consumer_poll(rk_consumer, 1000);
        if (mensaje_asignacion != NULL) {
            rd_kafka_message_destroy(mensaje_asignacion);
        }
        if (confirmar_offset_guardado(rk_consumer, topic_completado, particion_completada, offset_completado) != 0) {
            fprintf(stderr, "[RECUPERACION] No se pudo confirmar el resultado guardado en Kafka.\n");
            rd_kafka_topic_destroy(topic_peticiones);
            rd_kafka_destroy(rk_producer);
            rd_kafka_consumer_close(rk_consumer);
            rd_kafka_destroy(rk_consumer);
            return 1;
        }
        limpiar_checkpoint();
    } else if (hay_checkpoint) {
        linea_checkpoint_actual = linea_pendiente;
        if (strcmp(estado_checkpoint, "READY") == 0) {
            char solicitud_pendiente[256];
            snprintf(solicitud_pendiente, sizeof(solicitud_pendiente), "%s#%s", id_operador, estacion_pendiente);
            if (publicar_mensaje(rk_producer, topic_peticiones, solicitud_pendiente) != 0 ||
                guardar_checkpoint(estacion_pendiente, linea_pendiente, "WAITING") != 0) {
                fprintf(stderr, "[RECUPERACION] No se pudo reenviar la petición pendiente.\n");
                rd_kafka_topic_destroy(topic_peticiones);
                rd_kafka_destroy(rk_producer);
                rd_kafka_consumer_close(rk_consumer);
                rd_kafka_destroy(rk_consumer);
                return 1;
            }
        }
        printf("[RECUPERACION] Se reanuda la espera de la operación pendiente en %s.\n", estacion_pendiente);
        if (!esperar_respuesta(rk_consumer, id_operador, estacion_pendiente)) {
            printf("[RECUPERACION] La operación sigue pendiente. El estado se conserva para el siguiente arranque.\n");
            rd_kafka_topic_destroy(topic_peticiones);
            rd_kafka_destroy(rk_producer);
            rd_kafka_consumer_close(rk_consumer);
            rd_kafka_destroy(rk_consumer);
            return 0;
        }
        limpiar_checkpoint();
    } else {
        rd_kafka_message_t *mensaje_inicial = rd_kafka_consumer_poll(rk_consumer, 1000);
        if (mensaje_inicial != NULL) {
            if (!mensaje_inicial->err) {
                confirmar_mensaje(rk_consumer, mensaje_inicial);
            }
            rd_kafka_message_destroy(mensaje_inicial);
        }
    }

    if (fichero != NULL) {
        printf("[MODO AUTOMATICO] Leyendo fichero: %s\n", fichero);
        FILE *file = fopen(fichero, "r");
        if (!file) {
            perror("Error al abrir el fichero de rutinas");
            return 1;
        }

        char id_estacion[128];
        int numero_linea = 0;
        int continuar = 1;
        while (fgets(id_estacion, sizeof(id_estacion), file)) {
            numero_linea++;
            if (numero_linea <= linea_pendiente) {
                continue;
            }
            id_estacion[strcspn(id_estacion, "\r\n")] = '\0';
            if (strlen(id_estacion) == 0) {
                continue;
            }

            char payload[256];
            snprintf(payload, sizeof(payload), "%s#%s", id_operador, id_estacion);

            printf(">> Solicitando riego para: %s...\n", id_estacion);

            linea_checkpoint_actual = numero_linea;
            if (guardar_checkpoint(id_estacion, numero_linea, "READY") != 0) {
                fprintf(stderr, "[FO] No se pudo guardar el checkpoint; no se enviará la solicitud.\n");
                continuar = 0;
                break;
            }
            if (publicar_mensaje(rk_producer, topic_peticiones, payload) != 0 ||
                guardar_checkpoint(id_estacion, numero_linea, "WAITING") != 0) {
                fprintf(stderr, "[FO] No se pudo confirmar el envío; se conserva la petición para reintentar.\n");
                continuar = 0;
                break;
            }

            if (!esperar_respuesta(rk_consumer, id_operador, id_estacion)) {
                printf("[FO] La petición sigue pendiente. Se conserva el punto de reanudación.\n");
                continuar = 0;
                break;
            }
            limpiar_checkpoint();

            printf("[FO] Esperando 4 segundos antes de la siguiente peticion...\n\n");
            sleep(4);
        }
        fclose(file);
        if (continuar) {
            printf("[MODO AUTOMATICO] Fichero procesado al 100%%.\n");
        }
    } else {
        printf("[MODO MANUAL] Escribe un ID para solicitar riego, 'listar' para consultar estaciones o 'salir':\n");
        char entrada[128];
        while (1) {
            printf("\nFO-%s > ", id_operador);
            if (!fgets(entrada, sizeof(entrada), stdin)) {
                break;
            }

            entrada[strcspn(entrada, "\r\n")] = '\0';
            if (strcmp(entrada, "salir") == 0) {
                break;
            }
            if (strlen(entrada) == 0) {
                continue;
            }
            if (strcasecmp(entrada, "listar") == 0) {
                solicitar_estaciones_disponibles(rk_producer, topic_peticiones, rk_consumer, id_operador);
                continue;
            }

            char payload[256];
            snprintf(payload, sizeof(payload), "%s#%s", id_operador, entrada);

            linea_checkpoint_actual = -1;
            if (guardar_checkpoint(entrada, -1, "READY") != 0) {
                fprintf(stderr, "[FO] No se pudo guardar el checkpoint; no se enviará la solicitud.\n");
                continue;
            }
            if (publicar_mensaje(rk_producer, topic_peticiones, payload) != 0 ||
                guardar_checkpoint(entrada, -1, "WAITING") != 0) {
                fprintf(stderr, "[FO] No se pudo confirmar el envío; se conserva la petición para reintentar.\n");
                break;
            }

            if (!esperar_respuesta(rk_consumer, id_operador, entrada)) {
                printf("[FO] La petición sigue pendiente. Se conservará para reanudarla al reiniciar.\n");
                break;
            }
            limpiar_checkpoint();
            sleep(4);
        }
    }

    rd_kafka_topic_destroy(topic_peticiones);
    rd_kafka_destroy(rk_producer);
    rd_kafka_consumer_close(rk_consumer);
    rd_kafka_destroy(rk_consumer);
    return 0;
}
