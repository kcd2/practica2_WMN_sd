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
#include <signal.h>
#include <sqlite3.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

int s = -1;      // socket servidor TCP (estaciones)
int s_http = -1; // socket servidor HTTP (panel web)

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

void *hilo_consumidor_kafka(void *arg) {
    char *ip_kafka = (char *)arg;
    char errstr[512];

    // CONSUMIDOR Kafka
    rd_kafka_conf_t *conf_cons = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_cons, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf_cons, "group.id", "grupo_central", errstr, sizeof(errstr));

    rd_kafka_t *rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf_cons, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(rk_consumer);
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, "wm_peticiones", RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk_consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    // PRODUCTOR Kafka
    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));

    rd_kafka_topic_t *topic_fo = rd_kafka_topic_new(rk_producer, "wm_respuestas_fo", NULL);
    rd_kafka_topic_t *topic_ws = rd_kafka_topic_new(rk_producer, "wm_ordenes_ws", NULL);

    printf("[KAFKA] Hilo iniciado. Escuchando peticiones y eventos en wm_peticiones...\n");

    while (1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);

        if (rkmessage && !rkmessage->err) {
            char payload[1024];
            snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
            payload[strcspn(payload, "\r\n")] = '\0';

            char *campo1 = strtok(payload, "#");
            char *campo2 = strtok(NULL, "#");
            char *campo3 = strtok(NULL, "#");
            char *campo4 = strtok(NULL, "#");

            if (campo1 && campo2) {
                // Caso A: Telemetría en tiempo real enviada por WM_WS_E (TELEMETRIA#ID_EST#CAUDAL_LPM#VOLUMEN_L)
                if (strcmp(campo1, "TELEMETRIA") == 0 && campo3 && campo4) {
                    printf("[TELEMETRIA %s] Caudal actual: %s L/min | Volumen acumulado: %s L\n",
                           campo2, campo3, campo4);

                    sqlite3 *db;
                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        char sql_tel[256];
                        snprintf(sql_tel, sizeof(sql_tel),
                                 "UPDATE estaciones SET caudal = %s, volumen = %s WHERE id = '%s';",
                                 campo3, campo4, campo2);
                        sqlite3_exec(db, sql_tel, 0, 0, NULL);
                        sqlite3_close(db);
                    }
                }
                // Caso B: Fin de riego enviado por WM_WS_E (FIN_RIEGO#ID_EST#VOLUMEN_L#DURACION_S)
                else if (strcmp(campo1, "FIN_RIEGO") == 0) {
                    sqlite3 *db;
                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        char sql_up[256];
                        snprintf(sql_up, sizeof(sql_up),
                                 "UPDATE estaciones SET estado = 'DISPONIBLE', caudal = 0.0, volumen = 0.0, operario = NULL WHERE id = '%s' AND estado = 'REGANDO';",
                                 campo2);
                        sqlite3_exec(db, sql_up, 0, 0, NULL);
                        sqlite3_close(db);
                    }
                    printf("[CENTRAL] Fin de riego en %s (Volumen: %s L, Duracion: %s s). Estado -> DISPONIBLE.\n",
                           campo2, campo3 ? campo3 : "0", campo4 ? campo4 : "0");

                    char info[256];
                    snprintf(info, sizeof(info), "Fin de riego en %s (Vol: %s L, Tiempo: %s s)",
                             campo2, campo3 ? campo3 : "0", campo4 ? campo4 : "0");
                    guardar_ultima_peticion(info);

                    char msg_resumen[256];
                    snprintf(msg_resumen, sizeof(msg_resumen), "TODOS#%s#FIN_RIEGO (Vol: %s L, Tiempo: %s s)",
                             campo2, campo3 ? campo3 : "0", campo4 ? campo4 : "0");
                    rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                     msg_resumen, strlen(msg_resumen), NULL, 0, NULL);
                    rd_kafka_flush(rk_producer, 1000);
                }
                // Caso C: Solicitud normal de un Operario (ID_OPERARIO#ID_ESTACION)
                else {
                    char *id_operario = campo1;
                    char *id_estacion = campo2;
                    printf("\n[KAFKA] Peticion de riego recibida: Operario %s -> Estacion %s\n", id_operario, id_estacion);

                    sqlite3 *db;
                    sqlite3_stmt *stmt;

                    if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                        sqlite3_busy_timeout(db, 2000);
                        char sql[256];
                        snprintf(sql, sizeof(sql), "SELECT estado FROM estaciones WHERE id = '%s';", id_estacion);

                        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                            char msg_productor[256];
                            char info[256];

                            if (sqlite3_step(stmt) == SQLITE_ROW) {
                                const unsigned char *estado = sqlite3_column_text(stmt, 0);
                                printf("[CENTRAL] Estado actual de %s: %s\n", id_estacion, estado);

                                if (strcmp((const char *)estado, "DISPONIBLE") == 0) {
                                    printf("[CENTRAL] -> Peticion AUTORIZADA. Reservando estacion (REGANDO) y avisando a FO y WS...\n");

                                    char sql_update[256];
                                    snprintf(sql_update, sizeof(sql_update),
                                             "UPDATE estaciones SET estado = 'REGANDO', caudal = 0.0, volumen = 0.0, operario = '%s' WHERE id = '%s';",
                                             id_operario, id_estacion);
                                    sqlite3_exec(db, sql_update, 0, 0, NULL);

                                    snprintf(info, sizeof(info), "Operario %s solicitó %s -> AUTORIZADO", id_operario, id_estacion);
                                    guardar_ultima_peticion(info);

                                    snprintf(msg_productor, sizeof(msg_productor), "%s#INICIAR_RIEGO#%s", id_estacion, id_operario);
                                    rd_kafka_produce(topic_ws, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                     msg_productor, strlen(msg_productor), NULL, 0, NULL);

                                    snprintf(msg_productor, sizeof(msg_productor), "%s#%s#AUTORIZADO", id_operario, id_estacion);
                                    rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                     msg_productor, strlen(msg_productor), NULL, 0, NULL);
                                } else {
                                    printf("[CENTRAL] -> Peticion DENEGADA. Estacion en estado %s.\n", estado);
                                    snprintf(info, sizeof(info), "Operario %s solicitó %s -> DENEGADO (%s)", id_operario, id_estacion, estado);
                                    guardar_ultima_peticion(info);

                                    snprintf(msg_productor, sizeof(msg_productor), "%s#%s#DENEGADO", id_operario, id_estacion);
                                    rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                     msg_productor, strlen(msg_productor), NULL, 0, NULL);
                                }
                            } else {
                                printf("[CENTRAL] -> Error: La estacion %s no existe.\n", id_estacion);
                                snprintf(info, sizeof(info), "Operario %s solicitó %s -> ERROR_NO_EXISTE", id_operario, id_estacion);
                                guardar_ultima_peticion(info);

                                snprintf(msg_productor, sizeof(msg_productor), "%s#%s#ERROR_NO_EXISTE", id_operario, id_estacion);
                                rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                 msg_productor, strlen(msg_productor), NULL, 0, NULL);
                            }
                            sqlite3_finalize(stmt);
                            rd_kafka_flush(rk_producer, 1000);
                        }
                        sqlite3_close(db);
                    }
                }
            }
        }
        if (rkmessage) rd_kafka_message_destroy(rkmessage);
    }
    return NULL;
}

void *hilo_menu_central(void *arg) {
    char *ip_kafka = (char *)arg;
    char entrada[128];
    char errstr[512];

    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    rd_kafka_topic_t *topic_ws = rd_kafka_topic_new(rk_producer, "wm_ordenes_ws", NULL);

    sleep(3);

    while (1) {
        printf("\n--- MANDO CENTRAL ---\n");
        printf("1. Iniciar Riego Manualmente\n");
        printf("2. Bloquear Estacion (Poner Fuera de Servicio)\n");
        printf("3. Activar Estacion (Poner Disponible)\n");
        printf("Seleccione opcion: ");

        if (!fgets(entrada, sizeof(entrada), stdin)) {
            sleep(1);
            continue;
        }
        int opcion = atoi(entrada);

        if (opcion >= 1 && opcion <= 3) {
            printf("Introduzca ID de la estacion (ej. WS-01): ");
            char id_est[64];
            if (!fgets(id_est, sizeof(id_est), stdin)) continue;
            id_est[strcspn(id_est, "\r\n")] = '\0';

            sqlite3 *db;
            if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 2000);
                char sql[256];
                char *err_msg = 0;
                char msg_productor[256];
                char info[256];

                if (opcion == 1) {
                    sqlite3_stmt *stmt;
                    snprintf(sql, sizeof(sql), "SELECT estado FROM estaciones WHERE id = '%s';", id_est);
                    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                        if (sqlite3_step(stmt) == SQLITE_ROW) {
                            const char *est = (const char *)sqlite3_column_text(stmt, 0);
                            if (strcmp(est, "DISPONIBLE") == 0) {
                                snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'REGANDO', caudal = 0.0, volumen = 0.0, operario = 'CENTRAL' WHERE id = '%s';", id_est);
                                sqlite3_exec(db, sql, 0, 0, NULL);

                                snprintf(info, sizeof(info), "Mando Central inició riego en %s", id_est);
                                guardar_ultima_peticion(info);

                                snprintf(msg_productor, sizeof(msg_productor), "%s#INICIAR_RIEGO#CENTRAL", id_est);
                                rd_kafka_produce(topic_ws, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                 msg_productor, strlen(msg_productor), NULL, 0, NULL);
                                rd_kafka_flush(rk_producer, 1000);
                                printf(">> Orden de riego enviada a %s (Estado -> REGANDO).\n", id_est);
                            } else {
                                printf(">> No se puede iniciar riego: La estacion %s esta en estado %s.\n", id_est, est);
                            }
                        } else {
                            printf(">> Error: La estacion %s no esta registrada en la BD.\n", id_est);
                        }
                        sqlite3_finalize(stmt);
                    }
                }
                else if (opcion == 2) {
                    snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'FUERA_SERVICIO', caudal = 0.0, volumen = 0.0, operario = NULL WHERE id = '%s';", id_est);
                    sqlite3_exec(db, sql, 0, 0, &err_msg);

                    snprintf(info, sizeof(info), "Mando Central bloqueó %s (FUERA_SERVICIO)", id_est);
                    guardar_ultima_peticion(info);

                    snprintf(msg_productor, sizeof(msg_productor), "%s#DETENER_RIEGO", id_est);
                    rd_kafka_produce(topic_ws, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                     msg_productor, strlen(msg_productor), NULL, 0, NULL);
                    rd_kafka_flush(rk_producer, 1000);
                    printf(">> La estacion %s ha sido bloqueada (FUERA_SERVICIO) y se ha ordenado detener riego.\n", id_est);
                }
                else if (opcion == 3) {
                    snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE id = '%s';", id_est);
                    sqlite3_exec(db, sql, 0, 0, &err_msg);

                    snprintf(info, sizeof(info), "Mando Central activó %s (DISPONIBLE)", id_est);
                    guardar_ultima_peticion(info);
                    printf(">> La estacion %s vuelve a estar activada y DISPONIBLE.\n", id_est);
                }
                sqlite3_close(db);
            }
        } else {
            printf("Opcion no valida.\n");
        }
    }
    return NULL;
}

/* =========================================================================
 * HILO HTTP MÍNIMO DE SOLO LECTURA (/api/status y index.html)
 * ========================================================================= */
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
    listen(s_http, 5);
    printf("[PANEL WEB] Escuchando en http://0.0.0.0:%d\n", puerto_http);

    while (1) {
        tam_cli = sizeof(dir_cli);
        int cli = accept(s_http, (struct sockaddr *)&dir_cli, &tam_cli);
        if (cli == -1) continue;

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

                sqlite3 *db;
                if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                    sqlite3_busy_timeout(db, 2000);
                    sqlite3_stmt *stmt;
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

                            len += snprintf(json + len, sizeof(json) - len,
                                            "%s{\"id\":\"%s\",\"ubicacion\":\"%s\",\"estado\":\"%s\","
                                            "\"caudal\":%.2f,\"volumen\":%.2f,\"operario\":\"%s\"}",
                                            primero ? "" : ",",
                                            id ? id : "", ubi ? ubi : "", est ? est : "DESCONECTADA",
                                            caudal, vol, op ? op : "");
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

    sqlite3 *db_init;
    if (sqlite3_open("db_data/water_management.db", &db_init) == SQLITE_OK) {
        const char *sql_create = "CREATE TABLE IF NOT EXISTS estaciones ("
                                 "id TEXT PRIMARY KEY, "
                                 "ubicacion TEXT, "
                                 "estado TEXT DEFAULT 'DISPONIBLE', "
                                 "caudal REAL DEFAULT 0.0, "
                                 "volumen REAL DEFAULT 0.0, "
                                 "operario TEXT DEFAULT NULL); "
                                 "ALTER TABLE estaciones ADD COLUMN caudal REAL DEFAULT 0.0; "
                                 "ALTER TABLE estaciones ADD COLUMN volumen REAL DEFAULT 0.0; "
                                 "ALTER TABLE estaciones ADD COLUMN operario TEXT DEFAULT NULL; "
                                 "CREATE TABLE IF NOT EXISTS operarios ("
                                 "id TEXT PRIMARY KEY, "
                                 "nombre TEXT); "
                                 "UPDATE estaciones SET estado = 'DESCONECTADA', caudal = 0.0, volumen = 0.0, operario = NULL;";
        sqlite3_exec(db_init, sql_create, 0, 0, NULL);
        sqlite3_close(db_init);
        guardar_ultima_peticion("Central iniciada correctamente");
        printf("[CENTRAL] Base de datos SQLite inicializada con campos de telemetría y operario.\n");
    }

    pthread_t hilo_kafka;
    pthread_create(&hilo_kafka, NULL, hilo_consumidor_kafka, (void *)ip_kafka);

    pthread_t hilo_menu;
    pthread_create(&hilo_menu, NULL, hilo_menu_central, (void *)ip_kafka);

    pthread_t hilo_http;
    pthread_create(&hilo_http, NULL, hilo_http_panel, (void *)&puerto_http);

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
                        char *err_msg = 0;
                        char sql[512];

                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            snprintf(sql, sizeof(sql),
                                     "INSERT OR REPLACE INTO estaciones (id, ubicacion, estado, caudal, volumen, operario) VALUES ('%s', '%s', 'DISPONIBLE', 0.0, 0.0, NULL);",
                                     id_estacion, ubicacion);

                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) {
                                char info[256];
                                snprintf(info, sizeof(info), "Estación %s (%s) registrada como DISPONIBLE", id_estacion, ubicacion);
                                guardar_ultima_peticion(info);
                                printf(">>> BD: Estacion %s (%s) registrada y autenticada como DISPONIBLE <<<\n", id_estacion, ubicacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion registrada correctamente\n");
                            } else {
                                printf("Error BD: %s\n", err_msg);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al guardar en BD\n");
                                sqlite3_free(err_msg);
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else if (strcmp(tipo_mensaje, "AVERIA") == 0) {
                        sqlite3 *db;
                        char *err_msg = 0;
                        char sql[512];

                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'FUGA', caudal = 0.0, volumen = 0.0, operario = NULL WHERE id = '%s';", id_estacion);

                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) {
                                char info[256];
                                snprintf(info, sizeof(info), "Alerta TCP: Estación %s reporta FUGA", id_estacion);
                                guardar_ultima_peticion(info);
                                printf(">>> BD: Estacion %s reporta una FUGA (estado -> FUGA) <<<\n", id_estacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion actualizada a FUGA\n");
                            } else {
                                printf("Error BD: %s\n", err_msg);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al actualizar en BD\n");
                                sqlite3_free(err_msg);
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }
                    }
                    else if (strcmp(tipo_mensaje, "REPARADO") == 0) {
                        sqlite3 *db;
                        char *err_msg = 0;
                        char sql[512];

                        if (sqlite3_open("db_data/water_management.db", &db) == SQLITE_OK) {
                            sqlite3_busy_timeout(db, 2000);
                            snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE id = '%s';", id_estacion);

                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) {
                                char info[256];
                                snprintf(info, sizeof(info), "Aviso TCP: Estación %s REPARADA (DISPONIBLE)", id_estacion);
                                guardar_ultima_peticion(info);
                                printf(">>> BD: Estacion %s reporta que ha sido REPARADA (estado -> DISPONIBLE) <<<\n", id_estacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion actualizada a DISPONIBLE\n");
                            } else {
                                printf("Error BD: %s\n", err_msg);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al actualizar en BD\n");
                                sqlite3_free(err_msg);
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