// Archivo: WM_Central.c
// Compilación: gcc -o WM_Central WM_Central.c -lsqlite3 -lrdkafka -lpthread

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <time.h>
#include <stdint.h>
#include <sqlite3.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

int s = -1;      // socket servidor TCP (estaciones)
int s_http = -1; // socket servidor HTTP (panel web)
volatile int error_entrega_kafka = 0;

void finalizar(int senyal) {
    printf("\nApagando servidor WM_Central...\n");
    if (s != -1) close(s);
    if (s_http != -1) close(s_http);
    exit(0);
}

// Guarda una línea con la última petición o evento para mostrarla en el panel web
void guardar_ultima_peticion(const char *texto) {
    FILE *f = fopen("db_data/ultima_peticion.txt", "w");
    if (f) {
        fprintf(f, "%s", texto);
        fclose(f);
    }
}

void leer_ultima_peticion(char *buffer, size_t tam) {
    FILE *f = fopen("db_data/ultima_peticion.txt", "r");
    if (f) {
        if (fgets(buffer, (int)tam, f) != NULL) {
            buffer[strcspn(buffer, "\r\n\"\\")] = '\0';
            fclose(f);
            return;
        }
        fclose(f);
    }
    snprintf(buffer, tam, "Ninguna petición registrada todavía");
}

int columna_existe(sqlite3 *db, const char *tabla, const char *columna) {
    char sql[128];
    sqlite3_stmt *stmt = NULL;
    int existe = 0;
    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s);", tabla);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *nombre = sqlite3_column_text(stmt, 1);
            if (nombre && strcmp((const char *)nombre, columna) == 0) {
                existe = 1;
                break;
            }
        }
    }
    sqlite3_finalize(stmt);
    return existe;
}

int asegurar_columna(sqlite3 *db, const char *tabla, const char *columna, const char *tipo) {
    char sql[256];
    char *err_msg = NULL;
    if (columna_existe(db, tabla, columna)) return 0;
    snprintf(sql, sizeof(sql), "ALTER TABLE %s ADD COLUMN %s %s;", tabla, columna, tipo);
    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        const char *detalle_error = err_msg;
        if (detalle_error == NULL) {
            detalle_error = "desconocido";
        }
        fprintf(stderr, "Error SQLite al añadir %s.%s: %s\n", tabla, columna, detalle_error);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int cargar_estaciones_config(sqlite3 *db) {
    FILE *file = fopen("config/estaciones.txt", "r");
    if (file == NULL) return -1;
    sqlite3_stmt *stmt = NULL;
    const char *sql = "INSERT OR IGNORE INTO estaciones (id, ubicacion, estado, caudal, volumen, estado_pre_desconexion, ultimo_latido) VALUES (?, ?, 'DESCONECTADA', 0.0, 0.0, 'DISPONIBLE', 0);";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fclose(file);
        return -1;
    }
    char linea[512];
    while (fgets(linea, sizeof(linea), file) != NULL) {
        linea[strcspn(linea, "\r\n")] = '\0';
        if (linea[0] == '\0') continue;
        char *id = strtok(linea, "#");
        char *ubicacion = strtok(NULL, "#");
        if (id == NULL || ubicacion == NULL) continue;
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, ubicacion, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            fclose(file);
            return -1;
        }
    }
    sqlite3_finalize(stmt);
    fclose(file);
    return 0;
}

int cargar_operarios_config(sqlite3 *db) {
    FILE *file = fopen("config/operarios.txt", "r");
    if (file == NULL) return -1;
    sqlite3_stmt *stmt = NULL;
    const char *sql = "INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?);";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fclose(file);
        return -1;
    }
    char linea[512];
    while (fgets(linea, sizeof(linea), file) != NULL) {
        linea[strcspn(linea, "\r\n")] = '\0';
        if (linea[0] == '\0') continue;
        char *id = strtok(linea, "#");
        char *nombre = strtok(NULL, "#");
        if (id == NULL || nombre == NULL) continue;
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, nombre, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            fclose(file);
            return -1;
        }
    }
    sqlite3_finalize(stmt);
    fclose(file);
    return 0;
}

int insertar_outbox(sqlite3 *db, const char *topic, const char *payload) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = "INSERT INTO kafka_outbox (topic, payload, creado) VALUES (?, ?, CAST(strftime('%s','now') AS INTEGER));";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, payload, -1, SQLITE_TRANSIENT);
    int resultado = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (resultado != SQLITE_DONE) return -1;
    return 0;
}

int encolar_evento_central(const char *topic, const char *payload) {
    sqlite3 *db = NULL;
    if (sqlite3_open("db_data/water_management.db", &db) != SQLITE_OK) {
        if (db != NULL) sqlite3_close(db);
        return -1;
    }
    sqlite3_busy_timeout(db, 3000);
    int resultado = insertar_outbox(db, topic, payload);
    sqlite3_close(db);
    if (resultado != 0) fprintf(stderr, "No se pudo persistir un mensaje dirigido a Kafka.\n");
    return resultado;
}

int encolar_evento_transaccional(sqlite3 *db, const char *topic, const char *payload) {
    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) return -1;
    if (insertar_outbox(db, topic, payload) != 0) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }
    return 0;
}

void publicar_mensaje_fo(const char *texto) {
    encolar_evento_central("wm_respuestas_fo", texto);
}

void publicar_error_fo(const char *id_operario, const char *id_estacion, const char *causa, double volumen, int duracion) {
    char mensaje[512];
    snprintf(mensaje, sizeof(mensaje), "%s#%s#ERROR_AVERIA#%s#%.2f#%d",
             id_operario, id_estacion, causa, volumen, duracion);
    publicar_mensaje_fo(mensaje);
}

int finalizar_riego_central(const char *id_estacion, const char *volumen, const char *duracion, const char *causa) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    char id_operario[128] = "";
    char estado_anterior[64] = "";
    const char *volumen_texto = volumen;
    const char *duracion_texto = duracion;
    const char *causa_texto = causa;
    int resultado = -1;
    if (volumen_texto == NULL) volumen_texto = "0";
    if (duracion_texto == NULL) duracion_texto = "0";
    if (causa_texto == NULL) causa_texto = "NORMAL";
    if (sqlite3_open("db_data/water_management.db", &db) != SQLITE_OK) {
        if (db != NULL) sqlite3_close(db);
        return -1;
    }
    sqlite3_busy_timeout(db, 3000);
    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return -1;
    }
    if (sqlite3_prepare_v2(db, "SELECT operario, estado FROM estaciones WHERE id = ?;", -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        sqlite3_close(db);
        return -1;
    }
    sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
    int paso_consulta = sqlite3_step(stmt);
    if (paso_consulta == SQLITE_ROW) {
        const unsigned char *op = sqlite3_column_text(stmt, 0);
        const unsigned char *estado = sqlite3_column_text(stmt, 1);
        if (op != NULL) snprintf(id_operario, sizeof(id_operario), "%s", (const char *)op);
        if (estado != NULL) snprintf(estado_anterior, sizeof(estado_anterior), "%s", (const char *)estado);
    } else if (paso_consulta != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        sqlite3_close(db);
        return -1;
    }
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (id_operario[0] == '\0') {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        sqlite3_close(db);
        return 0;
    }
    const char *sql = "UPDATE estaciones SET estado = CASE WHEN ? = 'FUGA' THEN 'FUGA' WHEN ? = 'ENGINE_DESCONECTADO' THEN 'DESCONECTADA' WHEN estado = 'REGANDO' THEN 'DISPONIBLE' ELSE estado END, caudal = 0.0, volumen = ?, operario = NULL, riego_inicio = NULL, riego_fin = NULL, estado_pre_desconexion = CASE WHEN ? = 'ENGINE_DESCONECTADO' THEN 'DISPONIBLE' WHEN estado = 'DESCONECTADA' THEN 'DISPONIBLE' ELSE estado_pre_desconexion END WHERE id = ?;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, causa_texto, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, causa_texto, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 3, atof(volumen_texto));
        sqlite3_bind_text(stmt, 4, causa_texto, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 5, id_estacion, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(db) > 0) resultado = 0;
    }
    sqlite3_finalize(stmt);
    if (resultado == 0 && id_operario[0] != '\0' && strcmp(id_operario, "CENTRAL") != 0 && strcmp(id_operario, "MANUAL_WS") != 0) {
        if ((strcmp(causa_texto, "FUGA") == 0 || strcmp(causa_texto, "ENGINE_DESCONECTADO") == 0) &&
            strcmp(estado_anterior, "FUGA") != 0) {
            char error[512];
            snprintf(error, sizeof(error), "%s#%s#ERROR_AVERIA#%s#%.2f#%d",
                     id_operario, id_estacion, causa_texto, atof(volumen_texto), atoi(duracion_texto));
            if (insertar_outbox(db, "wm_respuestas_fo", error) != 0) resultado = -1;
        }
        char resumen[512];
        snprintf(resumen, sizeof(resumen), "%s#%s#FIN_RIEGO (Causa: %s, Vol: %s L, Tiempo: %s s)",
                 id_operario, id_estacion, causa_texto, volumen_texto, duracion_texto);
        if (resultado == 0 && insertar_outbox(db, "wm_respuestas_fo", resumen) != 0) resultado = -1;
    }
    if (resultado == 0 && sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK) {
        char info[256];
        snprintf(info, sizeof(info), "Fin de riego en %s (Vol: %s L, Tiempo: %s s)",
                 id_estacion, volumen_texto, duracion_texto);
        guardar_ultima_peticion(info);
    } else {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        resultado = -1;
    }
    sqlite3_close(db);
    return resultado;
}

int registrar_averia_central(sqlite3 *db, const char *id_estacion, const char *causa) {
    sqlite3_stmt *stmt = NULL;
    char estado_actual[64] = "";
    char operario_actual[128] = "";
    double volumen_actual = 0.0;
    int duracion_actual = 0;
    int resultado = -1;
    const char *causa_texto = causa;
    if (causa_texto == NULL || causa_texto[0] == '\0') causa_texto = "FUGA";
    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) return -1;
    if (sqlite3_prepare_v2(db, "SELECT estado, operario, volumen, riego_inicio FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *estado = sqlite3_column_text(stmt, 0);
            const unsigned char *operario = sqlite3_column_text(stmt, 1);
            if (estado != NULL) snprintf(estado_actual, sizeof(estado_actual), "%s", (const char *)estado);
            if (operario != NULL) snprintf(operario_actual, sizeof(operario_actual), "%s", (const char *)operario);
            volumen_actual = sqlite3_column_double(stmt, 2);
            sqlite3_int64 inicio = sqlite3_column_int64(stmt, 3);
            if (inicio > 0) duracion_actual = (int)(time(NULL) - (time_t)inicio);
        }
    }
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (estado_actual[0] == '\0') goto error_averia;
    const char *sql = "UPDATE estaciones SET estado = CASE WHEN ? = 'ENGINE_DESCONECTADO' THEN 'DESCONECTADA' ELSE 'FUGA' END, estado_pre_desconexion = CASE WHEN ? = 'ENGINE_DESCONECTADO' THEN 'DISPONIBLE' WHEN estado = 'DESCONECTADA' THEN 'FUGA' ELSE estado_pre_desconexion END, caudal = 0.0, riego_fin = CASE WHEN operario IS NOT NULL AND riego_fin IS NULL THEN CAST(strftime('%s','now') AS INTEGER) ELSE riego_fin END, ultimo_latido = CAST(strftime('%s','now') AS INTEGER) WHERE id = ?;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) goto error_averia;
    sqlite3_bind_text(stmt, 1, causa_texto, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, causa_texto, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, id_estacion, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE || sqlite3_changes(db) == 0) goto error_averia;
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (strcmp(estado_actual, "FUGA") != 0 &&
        (strcmp(estado_actual, "REGANDO") == 0 || strcmp(estado_actual, "DESCONECTADA") == 0) &&
        operario_actual[0] != '\0' && strcmp(operario_actual, "CENTRAL") != 0 && strcmp(operario_actual, "MANUAL_WS") != 0) {
        char mensaje[512];
        snprintf(mensaje, sizeof(mensaje), "%s#%s#ERROR_AVERIA#%s#%.2f#%d", operario_actual, id_estacion, causa_texto, volumen_actual, duracion_actual);
        if (insertar_outbox(db, "wm_respuestas_fo", mensaje) != 0) goto error_averia;
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto error_averia;
    resultado = 0;
    return resultado;

error_averia:
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return -1;
}

int registrar_reparacion_central(sqlite3 *db, const char *id_estacion) {
    sqlite3_stmt *stmt = NULL;
    char operario_actual[128] = "";
    double volumen_actual = 0.0;
    int duracion_actual = 0;
    int existe = 0;
    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) return -1;
    if (sqlite3_prepare_v2(db, "SELECT operario, volumen, riego_inicio, riego_fin FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *operario = sqlite3_column_text(stmt, 0);
            if (operario != NULL) snprintf(operario_actual, sizeof(operario_actual), "%s", (const char *)operario);
            volumen_actual = sqlite3_column_double(stmt, 1);
            sqlite3_int64 inicio = sqlite3_column_int64(stmt, 2);
            sqlite3_int64 fin = sqlite3_column_int64(stmt, 3);
            if (inicio > 0 && fin > 0) duracion_actual = (int)(fin - inicio);
            else if (inicio > 0) duracion_actual = (int)(time(NULL) - (time_t)inicio);
            existe = 1;
        }
    }
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (!existe) goto error_reparacion;
    const char *sql = "UPDATE estaciones SET estado = CASE WHEN estado = 'FUERA_SERVICIO' THEN 'FUERA_SERVICIO' ELSE 'DISPONIBLE' END, estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN 'DISPONIBLE' ELSE estado_pre_desconexion END, ultimo_latido = CAST(strftime('%s','now') AS INTEGER) WHERE id = ?;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) goto error_reparacion;
    sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE || sqlite3_changes(db) == 0) goto error_reparacion;
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (operario_actual[0] != '\0') {
        if (sqlite3_prepare_v2(db, "UPDATE estaciones SET operario = NULL, riego_inicio = NULL, riego_fin = NULL, caudal = 0.0 WHERE id = ?;", -1, &stmt, NULL) != SQLITE_OK) goto error_reparacion;
        sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) != SQLITE_DONE) goto error_reparacion;
        sqlite3_finalize(stmt);
        stmt = NULL;
        if (strcmp(operario_actual, "CENTRAL") != 0 && strcmp(operario_actual, "MANUAL_WS") != 0) {
            char resumen[512];
            snprintf(resumen, sizeof(resumen), "%s#%s#FIN_RIEGO (Recuperado tras averia, Vol: %.2f L, Tiempo: %d s)", operario_actual, id_estacion, volumen_actual, duracion_actual);
            if (insertar_outbox(db, "wm_respuestas_fo", resumen) != 0) goto error_reparacion;
        }
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto error_reparacion;
    return 0;

error_reparacion:
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return -1;
}

void confirmar_entrega_kafka(rd_kafka_t *rk, const rd_kafka_message_t *mensaje, void *opaque) {
    (void)rk;
    (void)opaque;
    if (mensaje->err != RD_KAFKA_RESP_ERR_NO_ERROR) error_entrega_kafka = 1;
}

void *hilo_publicador_outbox(void *arg) {
    char *brokers = (char *)arg;
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "[KAFKA] No se pudo configurar el publicador de salida: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    rd_kafka_conf_set_dr_msg_cb(conf, confirmar_entrega_kafka);
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (producer == NULL) {
        fprintf(stderr, "[KAFKA] No se pudo crear el publicador de salida: %s\n", errstr);
        return NULL;
    }
    while (1) {
        sqlite3 *db = NULL;
        sqlite3_stmt *stmt = NULL;
        sqlite3_int64 id = 0;
        char topic_nombre[128] = "";
        char payload[1024] = "";
        int encontrado = 0;
        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
            sqlite3_busy_timeout(db, 3000);
            if (sqlite3_prepare_v2(db, "SELECT id, topic, payload FROM kafka_outbox WHERE entregado = 0 ORDER BY id LIMIT 1;", -1, &stmt, NULL) == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
                id = sqlite3_column_int64(stmt, 0);
                const unsigned char *nombre = sqlite3_column_text(stmt, 1);
                const unsigned char *contenido = sqlite3_column_text(stmt, 2);
                if (nombre != NULL && contenido != NULL) {
                    snprintf(topic_nombre, sizeof(topic_nombre), "%s", (const char *)nombre);
                    snprintf(payload, sizeof(payload), "%s", (const char *)contenido);
                    encontrado = 1;
                }
            }
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
        if (!encontrado) {
            sleep(1);
            continue;
        }
        rd_kafka_topic_t *topic = rd_kafka_topic_new(producer, topic_nombre, NULL);
        if (topic == NULL) {
            sleep(1);
            continue;
        }
        error_entrega_kafka = 0;
        int producido = rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                         payload, strlen(payload), NULL, 0, NULL);
        int vaciado = RD_KAFKA_RESP_ERR__FAIL;
        if (producido == 0) vaciado = rd_kafka_flush(producer, 5000);
        rd_kafka_topic_destroy(topic);
        if (producido == 0 && vaciado == RD_KAFKA_RESP_ERR_NO_ERROR && !error_entrega_kafka) {
            if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 3000);
                if (sqlite3_prepare_v2(db, "UPDATE kafka_outbox SET entregado = 1 WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK) {
                    sqlite3_bind_int64(stmt, 1, id);
                    if (sqlite3_step(stmt) != SQLITE_DONE) fprintf(stderr, "No se pudo marcar el mensaje Kafka como entregado.\n");
                }
                sqlite3_finalize(stmt);
                sqlite3_close(db);
            }
        } else {
            fprintf(stderr, "[KAFKA] Entrega pendiente; el mensaje permanece en el outbox para reintento.\n");
            sleep(1);
        }
    }
    return NULL;
}

void *hilo_vigilancia_monitores(void *arg) {
    (void)arg;
    while (1) {
        sleep(1);
        sqlite3 *db = NULL;
        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
            sqlite3_busy_timeout(db, 2000);
            const char *sql = "UPDATE estaciones SET estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN estado_pre_desconexion ELSE estado END, estado = 'DESCONECTADA', caudal = 0.0 WHERE estado != 'DESCONECTADA' AND (ultimo_latido IS NULL OR CAST(strftime('%s','now') AS INTEGER) - ultimo_latido >= 5);";
            if (sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK) {
                fprintf(stderr, "[CENTRAL] No se pudieron actualizar los estados por vencimiento de latido.\n");
            }
            sqlite3_close(db);
        }
    }
    return NULL;
}

void *hilo_consumidor_kafka(void *arg) {
    char *ip_kafka = (char *)arg;
    char errstr[512];

    // CONSUMIDOR Kafka
    rd_kafka_conf_t *conf_cons = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_cons, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "group.id", "grupo_central", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "enable.auto.commit", "false", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "auto.offset.reset", "earliest", errstr, sizeof(errstr));

    rd_kafka_t *rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf_cons, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(rk_consumer);
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, "wm_peticiones", RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk_consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    printf("[KAFKA] Hilo iniciado. Escuchando peticiones y eventos en wm_peticiones...\n");

    while (1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);
        int procesado_ok = 0;

        if (rkmessage && !rkmessage->err) {
            procesado_ok = 1;
            char payload[1024];
            snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
            payload[strcspn(payload, "\r\n")] = '\0';

            char *campo1 = strtok(payload, "#");
            char *campo2 = strtok(NULL, "#");
            char *campo3 = strtok(NULL, "#");
            char *campo4 = strtok(NULL, "#");
            char *campo5 = strtok(NULL, "#");

            if (campo1 && campo2) {
                // Caso A: Telemetría en tiempo real enviada por WM_WS_E (TELEMETRIA#ID_EST#CAUDAL_LPM#VOLUMEN_L)
                if (strcmp(campo1, "TELEMETRIA") == 0 && campo3 && campo4) {
                    printf("[TELEMETRIA %s] Caudal actual: %s L/min | Volumen acumulado: %s L\n",
                           campo2, campo3, campo4);

                    sqlite3 *db;
                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        int transaccion_ok = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) == SQLITE_OK;
                        sqlite3_stmt *stmt_tel = NULL;
                        if (transaccion_ok && sqlite3_prepare_v2(db, "UPDATE estaciones SET caudal = ?, volumen = ? WHERE id = ?;", -1, &stmt_tel, NULL) == SQLITE_OK) {
                            sqlite3_bind_double(stmt_tel, 1, atof(campo3));
                            sqlite3_bind_double(stmt_tel, 2, atof(campo4));
                            sqlite3_bind_text(stmt_tel, 3, campo2, -1, SQLITE_TRANSIENT);
                            if (sqlite3_step(stmt_tel) != SQLITE_DONE) transaccion_ok = 0;
                        } else {
                            transaccion_ok = 0;
                        }
                        sqlite3_finalize(stmt_tel);
                        sqlite3_stmt *stmt = NULL;
                        int consulta_operario_ok = transaccion_ok && sqlite3_prepare_v2(db, "SELECT operario FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK;
                        if (consulta_operario_ok) {
                            sqlite3_bind_text(stmt, 1, campo2, -1, SQLITE_TRANSIENT);
                            int paso_operario = sqlite3_step(stmt);
                            if (paso_operario == SQLITE_ROW) {
                                const unsigned char *operario = sqlite3_column_text(stmt, 0);
                                if (operario != NULL && strcmp((const char *)operario, "CENTRAL") != 0 &&
                                    strcmp((const char *)operario, "MANUAL_WS") != 0) {
                                    char mensaje_fo[256];
                                    snprintf(mensaje_fo, sizeof(mensaje_fo), "%s#%s#TELEMETRIA#%s#%s",
                                             (const char *)operario, campo2, campo3, campo4);
                                    if (insertar_outbox(db, "wm_respuestas_fo", mensaje_fo) != 0) transaccion_ok = 0;
                                }
                            } else transaccion_ok = 0;
                        } else {
                            transaccion_ok = 0;
                        }
                        sqlite3_finalize(stmt);
                        if (transaccion_ok) {
                            if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
                                sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                                procesado_ok = 0;
                            }
                        } else {
                            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                            fprintf(stderr, "[CENTRAL] Telemetría no guardada; el offset Kafka se volverá a leer.\n");
                            procesado_ok = 0;
                        }
                        sqlite3_close(db);
                    } else {
                        procesado_ok = 0;
                    }
                }
                else if (strcmp(campo2, "INICIO_RIEGO") == 0 && campo3 && strcmp(campo3, "MANUAL_WS") == 0) {
                    sqlite3 *db = NULL;
                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        int inicio_guardado = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) == SQLITE_OK;
                        sqlite3_stmt *stmt = NULL;
                        const char *sql = "UPDATE estaciones SET estado = 'REGANDO', caudal = 0.0, volumen = 0.0, operario = 'MANUAL_WS', riego_inicio = CAST(strftime('%s','now') AS INTEGER), riego_fin = NULL WHERE id = ? AND estado = 'DISPONIBLE';";
                        if (inicio_guardado && sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                            sqlite3_bind_text(stmt, 1, campo1, -1, SQLITE_TRANSIENT);
                            inicio_guardado = sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(db) > 0;
                        } else {
                            inicio_guardado = 0;
                        }
                        sqlite3_finalize(stmt);
                        if (inicio_guardado) {
                            if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
                                sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                                procesado_ok = 0;
                            }
                        } else {
                            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                            procesado_ok = 0;
                        }
                        sqlite3_close(db);
                    } else {
                        procesado_ok = 0;
                    }
                }
                // Caso B: Fin de riego enviado por WM_WS_E (FIN_RIEGO#ID_EST#VOLUMEN_L#DURACION_S)
                else if (strcmp(campo1, "FIN_RIEGO") == 0) {
                    if (finalizar_riego_central(campo2, campo3, campo4, campo5) != 0) {
                        fprintf(stderr, "[CENTRAL] No se pudo cerrar el riego; se conservará el evento para reintento.\n");
                        procesado_ok = 0;
                    } else {
                        printf("[CENTRAL] Fin de riego recibido para %s.\n", campo2);
                    }
                }
                else if (strcmp(campo2, "LISTAR") == 0) {
                    sqlite3 *db;
                    int cantidad = 0;
                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        sqlite3_stmt *stmt = NULL;
                        const char *sql_lista = "SELECT id, ubicacion, estado FROM estaciones WHERE estado = 'DISPONIBLE' ORDER BY id;";
                        int lista_ok = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) == SQLITE_OK;
                        if (lista_ok && sqlite3_prepare_v2(db, sql_lista, -1, &stmt, NULL) == SQLITE_OK) {
                            int paso_lista = SQLITE_OK;
                            while ((paso_lista = sqlite3_step(stmt)) == SQLITE_ROW) {
                                char id[128];
                                char ubicacion[256];
                                char estado[64];
                                const unsigned char *v_id = sqlite3_column_text(stmt, 0);
                                const unsigned char *v_ubicacion = sqlite3_column_text(stmt, 1);
                                const unsigned char *v_estado = sqlite3_column_text(stmt, 2);
                                const char *id_texto = "";
                                const char *ubicacion_texto = "";
                                const char *estado_texto = "";
                                if (v_id != NULL) {
                                    id_texto = (const char *)v_id;
                                }
                                if (v_ubicacion != NULL) {
                                    ubicacion_texto = (const char *)v_ubicacion;
                                }
                                if (v_estado != NULL) {
                                    estado_texto = (const char *)v_estado;
                                }
                                snprintf(id, sizeof(id), "%s", id_texto);
                                snprintf(ubicacion, sizeof(ubicacion), "%s", ubicacion_texto);
                                snprintf(estado, sizeof(estado), "%s", estado_texto);
                                char respuesta_lista[512];
                                snprintf(respuesta_lista, sizeof(respuesta_lista), "%s#LISTA#%s#%s#%s",
                                         campo1, id, ubicacion, estado);
                                if (insertar_outbox(db, "wm_respuestas_fo", respuesta_lista) != 0) lista_ok = 0;
                                cantidad++;
                            }
                            if (paso_lista != SQLITE_DONE) lista_ok = 0;
                        } else {
                            lista_ok = 0;
                        }
                        sqlite3_finalize(stmt);
                        char fin_lista[128];
                        snprintf(fin_lista, sizeof(fin_lista), "%s#LISTA_FIN#%d", campo1, cantidad);
                        if (lista_ok && insertar_outbox(db, "wm_respuestas_fo", fin_lista) == 0) {
                            if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
                                sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                                lista_ok = 0;
                            }
                        } else {
                            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                            lista_ok = 0;
                        }
                        sqlite3_close(db);
                        if (!lista_ok) procesado_ok = 0;
                    }
                    else {
                        procesado_ok = 0;
                    }
                }
                // Caso C: Solicitud normal de un Operario (ID_OPERARIO#ID_ESTACION)
                else {
                    char *id_operario = campo1;
                    char *id_estacion = campo2;
                    printf("\n[KAFKA] Peticion de riego recibida: Operario %s -> Estacion %s\n", id_operario, id_estacion);
                    rd_kafka_timestamp_type_t tipo_timestamp;
                    int64_t timestamp_peticion = rd_kafka_message_timestamp(rkmessage, &tipo_timestamp);
                    int solicitud_retrasada = tipo_timestamp != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE && timestamp_peticion > 0 &&
                                              (long long)time(NULL) * 1000 - (long long)timestamp_peticion > 45000;
                    if (solicitud_retrasada) {
                        sqlite3 *db_retrasada = NULL;
                        if (sqlite3_open("db_data/water_management.db", &db_retrasada) == SQLITE_OK) {
                            sqlite3_busy_timeout(db_retrasada, 2000);
                            char respuesta_retrasada[256];
                            snprintf(respuesta_retrasada, sizeof(respuesta_retrasada), "%s#%s#ERROR_CENTRAL_CAIDA", id_operario, id_estacion);
                            if (encolar_evento_transaccional(db_retrasada, "wm_respuestas_fo", respuesta_retrasada) != 0) procesado_ok = 0;
                            printf("[CENTRAL] Solicitud de %s rechazada por superar 45 segundos en cola.\n", id_estacion);
                            sqlite3_close(db_retrasada);
                        } else {
                            procesado_ok = 0;
                        }
                    } else {
                    sqlite3 *db = NULL;
                    sqlite3_stmt *stmt = NULL;

                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        int operario_autorizado = 0;
                        int consulta_operario_ok = sqlite3_prepare_v2(db, "SELECT 1 FROM operarios WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK;
                        if (consulta_operario_ok) {
                            sqlite3_bind_text(stmt, 1, id_operario, -1, SQLITE_TRANSIENT);
                            int paso = sqlite3_step(stmt);
                            if (paso == SQLITE_ROW) operario_autorizado = 1;
                            else if (paso != SQLITE_DONE) consulta_operario_ok = 0;
                        }
                        sqlite3_finalize(stmt);
                        stmt = NULL;

                        if (!consulta_operario_ok) {
                            procesado_ok = 0;
                        } else if (!operario_autorizado) {
                            char msg_error[256];
                            snprintf(msg_error, sizeof(msg_error), "%s#%s#ERROR_OPERARIO", id_operario, id_estacion);
                            if (encolar_evento_transaccional(db, "wm_respuestas_fo", msg_error) != 0) procesado_ok = 0;
                            printf("[CENTRAL] Peticion rechazada: el operario %s no esta registrado.\n", id_operario);
                        } else {
                            char estado_texto[64] = "";
                            int estacion_existe = 0;
                            int consulta_estacion_ok = sqlite3_prepare_v2(db, "SELECT estado FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK;
                            if (consulta_estacion_ok) {
                                sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
                                int paso = sqlite3_step(stmt);
                                if (paso == SQLITE_ROW) {
                                    const unsigned char *estado = sqlite3_column_text(stmt, 0);
                                    if (estado != NULL) snprintf(estado_texto, sizeof(estado_texto), "%s", (const char *)estado);
                                    estacion_existe = 1;
                                } else if (paso != SQLITE_DONE) {
                                    consulta_estacion_ok = 0;
                                }
                            }
                            sqlite3_finalize(stmt);
                            stmt = NULL;
                            if (!consulta_estacion_ok) {
                                procesado_ok = 0;
                            } else if (!estacion_existe) {
                                char msg_error[256];
                                snprintf(msg_error, sizeof(msg_error), "%s#%s#ERROR_NO_EXISTE", id_operario, id_estacion);
                                if (encolar_evento_transaccional(db, "wm_respuestas_fo", msg_error) != 0) procesado_ok = 0;
                            } else if (strcmp(estado_texto, "DISPONIBLE") == 0) {
                                char mensaje_orden[256];
                                char mensaje_fo[256];
                                int autorizado = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) == SQLITE_OK;
                                sqlite3_stmt *stmt_update = NULL;
                                if (autorizado && sqlite3_prepare_v2(db,
                                    "UPDATE estaciones SET estado = 'REGANDO', caudal = 0.0, volumen = 0.0, operario = ?, riego_inicio = CAST(strftime('%s','now') AS INTEGER), riego_fin = NULL WHERE id = ? AND estado = 'DISPONIBLE';",
                                    -1, &stmt_update, NULL) == SQLITE_OK) {
                                    sqlite3_bind_text(stmt_update, 1, id_operario, -1, SQLITE_TRANSIENT);
                                    sqlite3_bind_text(stmt_update, 2, id_estacion, -1, SQLITE_TRANSIENT);
                                    autorizado = sqlite3_step(stmt_update) == SQLITE_DONE && sqlite3_changes(db) > 0;
                                } else {
                                    autorizado = 0;
                                }
                                sqlite3_finalize(stmt_update);
                                if (autorizado) {
                                    snprintf(mensaje_orden, sizeof(mensaje_orden), "%s#INICIAR_RIEGO#%s", id_estacion, id_operario);
                                    snprintf(mensaje_fo, sizeof(mensaje_fo), "%s#%s#AUTORIZADO", id_operario, id_estacion);
                                    if (insertar_outbox(db, "wm_ordenes_ws", mensaje_orden) == 0 &&
                                        insertar_outbox(db, "wm_respuestas_fo", mensaje_fo) == 0 &&
                                        sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK) {
                                        char info[256];
                                        snprintf(info, sizeof(info), "Operario %s solicitó %s -> AUTORIZADO", id_operario, id_estacion);
                                        guardar_ultima_peticion(info);
                                    } else {
                                        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                                        procesado_ok = 0;
                                    }
                                } else {
                                    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                                    char mensaje_denegado[256];
                                    snprintf(mensaje_denegado, sizeof(mensaje_denegado), "%s#%s#DENEGADO", id_operario, id_estacion);
                                    if (encolar_evento_transaccional(db, "wm_respuestas_fo", mensaje_denegado) != 0) procesado_ok = 0;
                                }
                            } else if (strcmp(estado_texto, "REGANDO") == 0) {
                                char operario_actual[128] = "";
                                int consulta_actual_ok = sqlite3_prepare_v2(db, "SELECT operario FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK;
                                if (consulta_actual_ok) {
                                    sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
                                    int paso = sqlite3_step(stmt);
                                    if (paso == SQLITE_ROW) {
                                        const unsigned char *operario = sqlite3_column_text(stmt, 0);
                                        if (operario != NULL) snprintf(operario_actual, sizeof(operario_actual), "%s", (const char *)operario);
                                    } else {
                                        consulta_actual_ok = 0;
                                    }
                                }
                                sqlite3_finalize(stmt);
                                stmt = NULL;
                                if (!consulta_actual_ok) {
                                    procesado_ok = 0;
                                } else if (strcmp(operario_actual, id_operario) == 0) {
                                    char mensaje_autorizado[256];
                                    snprintf(mensaje_autorizado, sizeof(mensaje_autorizado), "%s#%s#AUTORIZADO", id_operario, id_estacion);
                                    if (encolar_evento_transaccional(db, "wm_respuestas_fo", mensaje_autorizado) != 0) procesado_ok = 0;
                                } else {
                                    char mensaje_denegado[256];
                                    snprintf(mensaje_denegado, sizeof(mensaje_denegado), "%s#%s#DENEGADO", id_operario, id_estacion);
                                    if (encolar_evento_transaccional(db, "wm_respuestas_fo", mensaje_denegado) != 0) procesado_ok = 0;
                                }
                            } else {
                                char mensaje_denegado[256];
                                snprintf(mensaje_denegado, sizeof(mensaje_denegado), "%s#%s#DENEGADO", id_operario, id_estacion);
                                if (encolar_evento_transaccional(db, "wm_respuestas_fo", mensaje_denegado) != 0) procesado_ok = 0;
                                char info[256];
                                snprintf(info, sizeof(info), "Operario %s solicitó %s -> DENEGADO (%s)", id_operario, id_estacion, estado_texto);
                                guardar_ultima_peticion(info);
                            }
                        }
                        sqlite3_close(db);
                    } else {
                        procesado_ok = 0;
                    }
                    }
                }
            }
        }
        if (rkmessage) {
            if (!rkmessage->err && procesado_ok) {
                rd_kafka_resp_err_t confirmacion = rd_kafka_commit_message(rk_consumer, rkmessage, 0);
                if (confirmacion != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    fprintf(stderr, "[KAFKA] No se pudo confirmar el offset; el evento se volverá a leer.\n");
                }
            }
            rd_kafka_message_destroy(rkmessage);
        }
    }
    return NULL;
}

int ejecutar_orden_central(sqlite3 *db, int opcion, const char *id_estacion) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = NULL;
    char mensaje[256];
    char info[256];
    int codigo;

    if (opcion == 1) {
        sql = "UPDATE estaciones SET estado = 'REGANDO', caudal = 0.0, volumen = 0.0, operario = 'CENTRAL', riego_inicio = CAST(strftime('%s','now') AS INTEGER), riego_fin = NULL WHERE id = ? AND estado = 'DISPONIBLE';";
    } else if (opcion == 2) {
        sql = "UPDATE estaciones SET estado = CASE WHEN estado = 'DESCONECTADA' THEN 'DESCONECTADA' ELSE 'FUERA_SERVICIO' END, estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN 'FUERA_SERVICIO' ELSE estado_pre_desconexion END, caudal = 0.0 WHERE id = ?;";
    } else {
        sql = "UPDATE estaciones SET estado = CASE WHEN estado = 'DESCONECTADA' THEN 'DESCONECTADA' ELSE 'DISPONIBLE' END, estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN 'DISPONIBLE' ELSE estado_pre_desconexion END WHERE id = ? AND (estado = 'FUERA_SERVICIO' OR (estado = 'DESCONECTADA' AND estado_pre_desconexion = 'FUERA_SERVICIO'));";
    }

    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }
    sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
    codigo = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (codigo != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }

    int cambios = sqlite3_changes(db);
    if (opcion == 1 && cambios > 0) {
        snprintf(mensaje, sizeof(mensaje), "%s#INICIAR_RIEGO#CENTRAL", id_estacion);
        if (insertar_outbox(db, "wm_ordenes_ws", mensaje) != 0) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return -1;
        }
    } else if (opcion == 2 && cambios > 0) {
        snprintf(mensaje, sizeof(mensaje), "%s#FUERA_SERVICIO#DETENER_RIEGO", id_estacion);
        if (insertar_outbox(db, "wm_ordenes_ws", mensaje) != 0) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return -1;
        }
    } else if (opcion == 3 && cambios > 0) {
        snprintf(mensaje, sizeof(mensaje), "%s#ACTIVAR_ESTACION", id_estacion);
        if (insertar_outbox(db, "wm_ordenes_ws", mensaje) != 0) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return -1;
        }
    }
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }
    if (cambios > 0) {
        if (opcion == 1) {
            snprintf(info, sizeof(info), "Mando Central inició riego en %s", id_estacion);
            printf(">> Orden de riego enviada a %s (Estado -> REGANDO).\n", id_estacion);
        } else if (opcion == 2) {
            snprintf(info, sizeof(info), "Mando Central bloqueó %s (FUERA_SERVICIO)", id_estacion);
            printf(">> La estación %s ha sido bloqueada y se ha ordenado detener el riego.\n", id_estacion);
        } else {
            snprintf(info, sizeof(info), "Mando Central activó %s (DISPONIBLE)", id_estacion);
            printf(">> La estación %s vuelve a estar activada.\n", id_estacion);
        }
        guardar_ultima_peticion(info);
    } else {
        printf(">> No se pudo aplicar la opción %d a %s: no está registrada o su estado no lo permite.\n",
               opcion, id_estacion);
    }
    return cambios;
}

void *hilo_menu_central(void *arg) {
    char *ip_kafka = (char *)arg;
    char entrada[128];
    (void)ip_kafka;

    sleep(3);

    while (1) {
        printf("\n--- MANDO CENTRAL ---\n");
        printf("1. Iniciar Riego Manualmente\n");
        printf("2. Bloquear Estación (Fuera de Servicio)\n");
        printf("3. Activar Estación\n");
        printf("Seleccione opción: ");

        if (!fgets(entrada, sizeof(entrada), stdin)) {
            sleep(1);
            continue;
        }
        int opcion = atoi(entrada);

        if (opcion >= 1 && opcion <= 3) {
            printf("Introduzca el ID de la estación o TODAS: ");
            char destino[128];
            if (!fgets(destino, sizeof(destino), stdin)) continue;
            destino[strcspn(destino, "\r\n")] = '\0';

            sqlite3 *db = NULL;
            if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 2000);
                sqlite3_stmt *stmt = NULL;
                const char *consulta;
                if (strcasecmp(destino, "TODAS") == 0) {
                    consulta = "SELECT id FROM estaciones ORDER BY id;";
                } else {
                    consulta = "SELECT id FROM estaciones WHERE id = ?;";
                }
                char ids[256][128];
                int cantidad = 0;
                if (sqlite3_prepare_v2(db, consulta, -1, &stmt, NULL) == SQLITE_OK) {
                    if (strcasecmp(destino, "TODAS") != 0) {
                        sqlite3_bind_text(stmt, 1, destino, -1, SQLITE_TRANSIENT);
                    }
                    while (cantidad < 256 && sqlite3_step(stmt) == SQLITE_ROW) {
                        const unsigned char *id = sqlite3_column_text(stmt, 0);
                        if (id) snprintf(ids[cantidad++], sizeof(ids[0]), "%s", (const char *)id);
                    }
                }
                sqlite3_finalize(stmt);
                if (cantidad == 0) {
                    printf(">> No se encontró ninguna estación autorizada para ese destino.\n");
                } else {
                    int aplicadas = 0;
                    for (int i = 0; i < cantidad; i++) {
                        int resultado = ejecutar_orden_central(db, opcion, ids[i]);
                        if (resultado > 0) aplicadas++;
                    }
                    printf(">> Acción aplicada a %d de %d estaciones seleccionadas.\n", aplicadas, cantidad);
                }
                sqlite3_close(db);
            }
        } else {
            printf("Opción no válida.\n");
        }
    }
    return NULL;
}

void *atender_cliente_http(void *arg) {
    int cli = *(int *)arg;
    free(arg);
    char req[1024];
    int n = read(cli, req, sizeof(req) - 1);
    if (n > 0) {
        req[n] = '\0';
        if (strncmp(req, "GET /api/status", 15) == 0) {
            char json[8192];
            char ult_pet[256];
            leer_ultima_peticion(ult_pet, sizeof(ult_pet));
            int len = snprintf(json, sizeof(json),
                               "{\"central\":\"OPERATIVA\",\"ultima_peticion\":\"%s\",\"stations\":[",
                               ult_pet);
            sqlite3 *db = NULL;
            if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 2000);
                sqlite3_stmt *stmt = NULL;
                const char *sql = "SELECT id, ubicacion, estado, caudal, volumen, operario FROM estaciones ORDER BY id;";
                if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                    int primero = 1;
                    while (sqlite3_step(stmt) == SQLITE_ROW && len < (int)sizeof(json) - 256) {
                        const char *id = (const char *)sqlite3_column_text(stmt, 0);
                        const char *ubi = (const char *)sqlite3_column_text(stmt, 1);
                        const char *est = (const char *)sqlite3_column_text(stmt, 2);
                        double caudal = sqlite3_column_double(stmt, 3);
                        double vol = sqlite3_column_double(stmt, 4);
                        const char *op = (const char *)sqlite3_column_text(stmt, 5);
                        const char *separador = "";
                        const char *id_texto = "";
                        const char *ubicacion_texto = "";
                        const char *estado_texto = "DESCONECTADA";
                        const char *operario_texto = "";
                        if (!primero) separador = ",";
                        if (id != NULL) id_texto = id;
                        if (ubi != NULL) ubicacion_texto = ubi;
                        if (est != NULL) estado_texto = est;
                        if (op != NULL) operario_texto = op;
                        len += snprintf(json + len, sizeof(json) - len,
                                        "%s{\"id\":\"%s\",\"ubicacion\":\"%s\",\"estado\":\"%s\","
                                        "\"caudal\":%.2f,\"volumen\":%.2f,\"operario\":\"%s\"}",
                                        separador, id_texto, ubicacion_texto, estado_texto,
                                        caudal, vol, operario_texto);
                        primero = 0;
                    }
                    sqlite3_finalize(stmt);
                }
                sqlite3_close(db);
            }
            len += snprintf(json + len, sizeof(json) - len, "]}");
            char cab[256];
            int hlen = snprintf(cab, sizeof(cab),
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
                                "Content-Length: %d\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n", len);
            write(cli, cab, hlen);
            write(cli, json, len);
        } else {
            FILE *f = fopen("dashboard/index.html", "rb");
            if (!f) f = fopen("index.html", "rb");
            if (f) {
                fseek(f, 0, SEEK_END);
                long fsize = ftell(f);
                fseek(f, 0, SEEK_SET);
                char *html = malloc(fsize);
                if (html) {
                    fread(html, 1, fsize, f);
                    char cab[256];
                    int hlen = snprintf(cab, sizeof(cab),
                                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                                        "Content-Length: %ld\r\nConnection: close\r\n\r\n", fsize);
                    write(cli, cab, hlen);
                    write(cli, html, fsize);
                    free(html);
                }
                fclose(f);
            } else {
                const char *err = "404 Not Found";
                char cab[256];
                int hlen = snprintf(cab, sizeof(cab),
                                    "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n"
                                    "Content-Length: 13\r\nConnection: close\r\n\r\n");
                write(cli, cab, hlen);
                write(cli, err, 13);
            }
        }
    }
    close(cli);
    return NULL;
}

void *hilo_http_panel(void *arg) {
    int puerto_http = *(int *)arg;
    struct sockaddr_in dir_http, dir_cli;
    socklen_t tam_cli;
    int opt = 1;
    s_http = socket(AF_INET, SOCK_STREAM, 0);
    if (s_http == -1) return NULL;
    setsockopt(s_http, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    dir_http.sin_family = AF_INET;
    dir_http.sin_port = htons(puerto_http);
    dir_http.sin_addr.s_addr = INADDR_ANY;
    if (bind(s_http, (struct sockaddr *)&dir_http, sizeof(dir_http)) == -1) {
        perror("Error en bind HTTP");
        close(s_http);
        return NULL;
    }
    listen(s_http, 32);
    printf("[PANEL WEB] Escuchando en http://0.0.0.0:%d\n", puerto_http);
    while (1) {
        tam_cli = sizeof(dir_cli);
        int cli = accept(s_http, (struct sockaddr *)&dir_cli, &tam_cli);
        if (cli == -1) continue;
        int *cliente = malloc(sizeof(int));
        if (cliente == NULL) {
            close(cli);
            continue;
        }
        *cliente = cli;
        pthread_t hilo_cliente;
        if (pthread_create(&hilo_cliente, NULL, atender_cliente_http, cliente) != 0) {
            close(cli);
            free(cliente);
            continue;
        }
        pthread_detach(hilo_cliente);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    sleep(10);

    struct sockaddr_in dir_servidor, dir_cliente;
    unsigned int long_dir_cliente;
    int s2, proceso;
    int opt = 1;
    char mensaje[1024], respuesta[1024];
    int recibidos;
    char *tipo_mensaje, *id_estacion, *ubicacion;

    if (argc < 3) {
        fprintf(stderr, "Uso: %s <PUERTO> <IP_KAFKA> [PUERTO_HTTP]\n", argv[0]);
        return 1;
    }

    char *servidor_puerto = argv[1];
    char *ip_kafka = argv[2];
    int puerto_http = 8081;
    if (argc >= 4) puerto_http = atoi(argv[3]);
    else if (getenv("PORT") && atoi(getenv("PORT")) != atoi(servidor_puerto)) puerto_http = atoi(getenv("PORT"));

    mkdir("db_data", 0777);

    printf("Iniciando WM_Central...\n");
    printf("- Puerto Sockets TCP: %s\n", servidor_puerto);
    printf("- Puerto Web HTTP:    %d\n", puerto_http);
    printf("- Broker Kafka:       %s\n", ip_kafka);

    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1) {
        perror("Error al crear socket");
        return 1;
    }

    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    dir_servidor.sin_family = AF_INET;
    dir_servidor.sin_port = htons(atoi(servidor_puerto));
    dir_servidor.sin_addr.s_addr = INADDR_ANY;

    if (bind(s, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1) {
        perror("Error al asociar socket con puerto");
        close(s);
        return 1;
    }

    if (listen(s, 5) == -1) {
        perror("Error al escuchar conexiones");
        close(s);
        return 1;
    }

    signal(SIGINT, finalizar);
    signal(SIGCHLD, SIG_IGN);

    sqlite3 *db_init = NULL;
    if (sqlite3_open("db_data/water_management.db", &db_init) != SQLITE_OK) {
        fprintf(stderr, "No se pudo abrir la base de datos SQLite.\n");
        if (db_init) sqlite3_close(db_init);
        return 1;
    }
    sqlite3_busy_timeout(db_init, 2000);
    const char *sql_create = "PRAGMA journal_mode=WAL;"
                             "PRAGMA synchronous=FULL;"
                             "CREATE TABLE IF NOT EXISTS estaciones ("
                             "id TEXT PRIMARY KEY, "
                             "ubicacion TEXT, "
                             "estado TEXT DEFAULT 'DISPONIBLE', "
                             "caudal REAL DEFAULT 0.0, "
                             "volumen REAL DEFAULT 0.0, "
                             "operario TEXT DEFAULT NULL, "
                             "estado_pre_desconexion TEXT DEFAULT 'DISPONIBLE', "
                             "ultimo_latido INTEGER DEFAULT 0, "
                             "riego_inicio INTEGER DEFAULT NULL, "
                             "riego_fin INTEGER DEFAULT NULL);"
                             "CREATE TABLE IF NOT EXISTS operarios ("
                             "id TEXT PRIMARY KEY, "
                             "nombre TEXT);"
                             "CREATE TABLE IF NOT EXISTS kafka_outbox ("
                             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                             "topic TEXT NOT NULL, "
                             "payload TEXT NOT NULL, "
                             "creado INTEGER NOT NULL, "
                             "entregado INTEGER NOT NULL DEFAULT 0);";
    char *err_init = NULL;
    int init_ok = sqlite3_exec(db_init, sql_create, NULL, NULL, &err_init) == SQLITE_OK;
    if (!init_ok) {
        const char *detalle_error = err_init;
        if (detalle_error == NULL) {
            detalle_error = "desconocido";
        }
        fprintf(stderr, "Error al crear el esquema SQLite: %s\n", detalle_error);
        sqlite3_free(err_init);
    }
    if (init_ok && asegurar_columna(db_init, "estaciones", "caudal", "REAL DEFAULT 0.0") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "volumen", "REAL DEFAULT 0.0") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "operario", "TEXT DEFAULT NULL") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "estado_pre_desconexion", "TEXT DEFAULT 'DISPONIBLE'") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "ultimo_latido", "INTEGER DEFAULT 0") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "riego_inicio", "INTEGER DEFAULT NULL") != 0) init_ok = 0;
    if (init_ok && asegurar_columna(db_init, "estaciones", "riego_fin", "INTEGER DEFAULT NULL") != 0) init_ok = 0;
    if (init_ok && cargar_estaciones_config(db_init) != 0) {
        fprintf(stderr, "No se pudo cargar config/estaciones.txt.\n");
        init_ok = 0;
    }
    if (init_ok && cargar_operarios_config(db_init) != 0) {
        fprintf(stderr, "No se pudo cargar config/operarios.txt.\n");
        init_ok = 0;
    }
    if (init_ok) {
        const char *sql_seed = "UPDATE estaciones SET estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN COALESCE(estado_pre_desconexion, 'DISPONIBLE') ELSE estado END, estado = 'DESCONECTADA', caudal = 0.0, ultimo_latido = 0;";
        if (sqlite3_exec(db_init, sql_seed, NULL, NULL, &err_init) != SQLITE_OK) {
            const char *detalle_error = err_init;
            if (detalle_error == NULL) {
                detalle_error = "desconocido";
            }
            fprintf(stderr, "Error al preparar las estaciones en SQLite: %s\n", detalle_error);
            sqlite3_free(err_init);
            init_ok = 0;
        }
    }
    sqlite3_close(db_init);
    if (!init_ok) return 1;
    guardar_ultima_peticion("Central iniciada correctamente");
    printf("[CENTRAL] Base de datos SQLite inicializada con campos de telemetría, operario y latido.\n");

    pthread_t hilo_kafka;
    pthread_create(&hilo_kafka, NULL, hilo_consumidor_kafka, (void *)ip_kafka);

    pthread_t hilo_outbox;
    pthread_create(&hilo_outbox, NULL, hilo_publicador_outbox, (void *)ip_kafka);

    pthread_t hilo_menu;
    pthread_create(&hilo_menu, NULL, hilo_menu_central, (void *)ip_kafka);

    pthread_t hilo_http;
    pthread_create(&hilo_http, NULL, hilo_http_panel, (void *)&puerto_http);

    pthread_t hilo_vigilancia;
    pthread_create(&hilo_vigilancia, NULL, hilo_vigilancia_monitores, NULL);

    while (1) {
        long_dir_cliente = sizeof(dir_cliente);
        s2 = accept(s, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
        if (s2 == -1) { continue; }

        proceso = fork();
        if (proceso == -1) { perror("Error al crear proceso"); close(s2); continue; }

        if (proceso == 0) {
            close(s);
            if (s_http != -1) close(s_http);

            recibidos = read(s2, mensaje, sizeof(mensaje) - 1);
            if (recibidos > 0) {
                mensaje[recibidos] = '\0';
                mensaje[strcspn(mensaje, "\r\n")] = '\0';
                printf("Mensaje TCP recibido [%d]: %s\n", recibidos, mensaje);

                tipo_mensaje = strtok(mensaje, "#");
                id_estacion = strtok(NULL, "#");
                ubicacion = strtok(NULL, "#");

                if (tipo_mensaje != NULL && id_estacion != NULL) {
                    if (strcmp(tipo_mensaje, "REGISTRO") == 0 && ubicacion != NULL) {
                        sqlite3 *db;

                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            sqlite3_stmt *stmt = NULL;
                            char ubicacion_autorizada[256] = "";
                            int autorizada = 0;
                            if (sqlite3_prepare_v2(db, "SELECT ubicacion FROM estaciones WHERE id = ?;", -1, &stmt, NULL) == SQLITE_OK) {
                                sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
                                if (sqlite3_step(stmt) == SQLITE_ROW) {
                                    const unsigned char *valor = sqlite3_column_text(stmt, 0);
                                    const char *ubicacion_texto = "";
                                    if (valor != NULL) {
                                        ubicacion_texto = (const char *)valor;
                                    }
                                    snprintf(ubicacion_autorizada, sizeof(ubicacion_autorizada), "%s", ubicacion_texto);
                                    autorizada = 1;
                                }
                            }
                            sqlite3_finalize(stmt);

                            if (autorizada && sqlite3_prepare_v2(db,
                                                                 "UPDATE estaciones SET estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' THEN estado_pre_desconexion ELSE estado END, estado = 'DESCONECTADA', caudal = 0.0, ultimo_latido = 0 WHERE id = ?;",
                                                                 -1, &stmt, NULL) == SQLITE_OK) {
                                sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
                                if (sqlite3_step(stmt) == SQLITE_DONE) {
                                    char info[256];
                                    snprintf(info, sizeof(info), "Estación %s (%s) conectando con Central", id_estacion, ubicacion_autorizada);
                                    guardar_ultima_peticion(info);
                                    printf(">>> BD: Estación %s (%s) autorizada; esperando latido del Monitor <<<\n", id_estacion, ubicacion_autorizada);
                                    snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion autorizada correctamente\n");
                                } else {
                                    snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al actualizar estacion\n");
                                }
                            } else if (!autorizada) {
                                printf(">>> REGISTRO RECHAZADO: estación %s no autorizada <<<\n", id_estacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Estacion no autorizada\n");
                            } else {
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al preparar registro\n");
                            }
                            sqlite3_finalize(stmt);
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else if (strcmp(tipo_mensaje, "LATIDO") == 0) {
                        sqlite3 *db = NULL;
                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            const char *estado_latido = "OK";
                            if (ubicacion != NULL && strcmp(ubicacion, "FUGA") == 0) estado_latido = "FUGA";
                            if (strcmp(estado_latido, "FUGA") == 0) {
                                if (registrar_averia_central(db, id_estacion, "FUGA") == 0) {
                                    snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Latido recibido\n");
                                } else {
                                    snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#No se pudo guardar la fuga\n");
                                }
                            } else {
                                sqlite3_stmt *stmt = NULL;
                                const char *sql_latido = "UPDATE estaciones SET ultimo_latido = CAST(strftime('%s','now') AS INTEGER), estado = CASE WHEN estado = 'FUERA_SERVICIO' THEN 'FUERA_SERVICIO' WHEN estado = 'DESCONECTADA' AND estado_pre_desconexion = 'FUERA_SERVICIO' THEN 'FUERA_SERVICIO' WHEN operario IS NOT NULL THEN 'REGANDO' WHEN estado = 'DESCONECTADA' THEN 'DISPONIBLE' WHEN estado = 'FUGA' THEN 'DISPONIBLE' ELSE estado END, estado_pre_desconexion = CASE WHEN estado = 'DESCONECTADA' AND estado_pre_desconexion = 'FUERA_SERVICIO' THEN 'FUERA_SERVICIO' WHEN estado = 'DESCONECTADA' THEN 'DISPONIBLE' ELSE estado_pre_desconexion END WHERE id = ?;";
                                if (sqlite3_prepare_v2(db, sql_latido, -1, &stmt, NULL) == SQLITE_OK) {
                                    sqlite3_bind_text(stmt, 1, id_estacion, -1, SQLITE_TRANSIENT);
                                    if (sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(db) > 0) {
                                        snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Latido recibido\n");
                                    } else {
                                        snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Estacion no autorizada\n");
                                    }
                                } else {
                                    snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al procesar latido\n");
                                }
                                sqlite3_finalize(stmt);
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else if (strcmp(tipo_mensaje, "AVERIA") == 0) {
                        sqlite3 *db = NULL;
                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            const char *causa = ubicacion;
                            if (causa == NULL || causa[0] == '\0') causa = "FUGA";
                            if (registrar_averia_central(db, id_estacion, causa) == 0) {
                                char info[256];
                                snprintf(info, sizeof(info), "Alerta TCP: Estación %s reporta %s", id_estacion, causa);
                                guardar_ultima_peticion(info);
                                printf(">>> BD: Estacion %s reporta %s <<<\n", id_estacion, causa);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Incidencia registrada\n");
                            } else {
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#No se pudo registrar la incidencia\n");
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else if (strcmp(tipo_mensaje, "REPARADO") == 0) {
                        sqlite3 *db = NULL;
                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            if (registrar_reparacion_central(db, id_estacion) == 0) {
                                char info[256];
                                snprintf(info, sizeof(info), "Aviso TCP: Estación %s REPARADA (DISPONIBLE)", id_estacion);
                                guardar_ultima_peticion(info);
                                printf(">>> BD: Estacion %s reporta que ha sido REPARADA (estado -> DISPONIBLE) <<<\n", id_estacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion actualizada a DISPONIBLE\n");
                            } else {
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#No se pudo registrar la reparacion\n");
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else {
                        snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Comando no reconocido\n");
                    }
                } else {
                    snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Formato de trama incorrecto\n");
                }

                write(s2, respuesta, strlen(respuesta));
            }

            close(s2);
            exit(0);
        } else {
            close(s2);
        }
    }
    close(s);
    return 0;
}
