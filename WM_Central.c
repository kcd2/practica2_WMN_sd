#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <sqlite3.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

int s; //socket

void finalizar(int senyal) {
    printf("\napagando servidor WM_Central...\n");
    close(s);
    exit(0);
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

    // PRODUCTOR Kafka
    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    
    // Topics donde escribirá la Central
    rd_kafka_topic_t *topic_fo = rd_kafka_topic_new(rk_producer, "wm_respuestas_fo", NULL);
    rd_kafka_topic_t *topic_ws = rd_kafka_topic_new(rk_producer, "wm_ordenes_ws", NULL);

    printf("[KAFKA] Hilo iniciado. Escuchando peticiones...\n");

    // Bucle infinito de procesamiento
    while(1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);
        
        if (rkmessage && !rkmessage->err) {
            char payload[1024];
            snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);
            printf("\n[KAFKA] Peticion recibida: %s\n", payload);

            // Formato esperado desde el FO: ID_OPERARIO#ID_ESTACION#COMANDO
            char *id_operario = strtok(payload, "#");
            char *id_estacion = strtok(NULL, "#");

            if (id_operario && id_estacion) {
                sqlite3 *db;
                sqlite3_stmt *stmt;
                
                if (sqlite3_open("water_management.db", &db) == SQLITE_OK) {
                    char sql[256];
                    snprintf(sql, sizeof(sql), "SELECT estado FROM estaciones WHERE id = '%s';", id_estacion);

                    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                        char msg_productor[256];

                        if (sqlite3_step(stmt) == SQLITE_ROW) {
                            const unsigned char *estado = sqlite3_column_text(stmt, 0);
                            printf("[CENTRAL] Estado actual de %s: %s\n", id_estacion, estado);

                            if (strcmp((const char *)estado, "DISPONIBLE") == 0) {
                                printf("[CENTRAL] -> Peticion AUTORIZADA. Avisando a FO y a WS...\n");
                                
                                // Enviar orden a la estación para que empiece a regar
                                snprintf(msg_productor, sizeof(msg_productor), "%s#INICIAR_RIEGO", id_estacion);
                                rd_kafka_produce(topic_ws, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                 msg_productor, strlen(msg_productor), NULL, 0, NULL);

                                // Enviar confirmación al operario
                                snprintf(msg_productor, sizeof(msg_productor), "%s#%s#AUTORIZADO", id_operario, id_estacion);
                                rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                 msg_productor, strlen(msg_productor), NULL, 0, NULL);
                            } else {
                                printf("[CENTRAL] -> Peticion DENEGADA. Estacion ocupada o con averia.\n");
                                
                                // Enviar denegación solo al operario
                                snprintf(msg_productor, sizeof(msg_productor), "%s#%s#DENEGADO", id_operario, id_estacion);
                                rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                                 msg_productor, strlen(msg_productor), NULL, 0, NULL);
                            }
                        } else {
                            printf("[CENTRAL] -> Error: La estacion %s no existe.\n", id_estacion);
                            snprintf(msg_productor, sizeof(msg_productor), "%s#%s#ERROR_NO_EXISTE", id_operario, id_estacion);
                            rd_kafka_produce(topic_fo, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                             msg_productor, strlen(msg_productor), NULL, 0, NULL);
                        }
                        sqlite3_finalize(stmt);
                        
                        // Forzar el envío inmediato de los mensajes generados
                        rd_kafka_flush(rk_producer, 1000);
                    }
                    sqlite3_close(db);
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

    // PRODUCTOR Kafka exclusivo para el menú
    rd_kafka_conf_t *conf_prod = rd_kafka_conf_new();
    rd_kafka_conf_set(conf_prod, "bootstrap.servers", ip_kafka, errstr, sizeof(errstr));
    rd_kafka_t *rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf_prod, errstr, sizeof(errstr));
    rd_kafka_topic_t *topic_ws = rd_kafka_topic_new(rk_producer, "wm_ordenes_ws", NULL);

    sleep(3); // Pausa breve para que el log de inicio termine de imprimir

    while(1) {
        printf("\n--- MANDO CENTRAL ---\n");
        printf("1. Iniciar Riego Manualmente\n");
        printf("2. Bloquear Estacion (Poner Fuera de Servicio)\n");
        printf("3. Activar Estacion (Poner Disponible)\n");
        printf("Seleccione opcion: ");
        
        if (!fgets(entrada, sizeof(entrada), stdin)) continue;
        int opcion = atoi(entrada);

        if (opcion >= 1 && opcion <= 3) {
            printf("Introduzca ID de la estacion (ej. WS-01): ");
            char id_est[64];
            if (!fgets(id_est, sizeof(id_est), stdin)) continue;
            id_est[strcspn(id_est, "\r\n")] = '\0'; // Limpiar salto de línea

            sqlite3 *db;
            if (sqlite3_open("water_management.db", &db) == SQLITE_OK) {
                char sql[256];
                char *err_msg = 0;
                char msg_productor[256];

                if (opcion == 1) {
                    snprintf(msg_productor, sizeof(msg_productor), "%s#INICIAR_RIEGO", id_est);
                    rd_kafka_produce(topic_ws, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                     msg_productor, strlen(msg_productor), NULL, 0, NULL);
                    rd_kafka_flush(rk_producer, 1000);
                    printf(">> Orden de riego enviada a %s a traves de Kafka.\n", id_est);
                } 
                else if (opcion == 2) {
                    snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'FUERA_SERVICIO' WHERE id = '%s';", id_est);
                    sqlite3_exec(db, sql, 0, 0, &err_msg);
                    printf(">> La estacion %s ha sido bloqueada correctamente.\n", id_est);
                }
                else if (opcion == 3) {
                    snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE id = '%s';", id_est);
                    sqlite3_exec(db, sql, 0, 0, &err_msg);
                    printf(">> La estacion %s vuelve a estar activada y disponible.\n", id_est);
                }
                sqlite3_close(db);
            }
        } else {
            printf("Opcion no valida.\n");
        }
    }
    return NULL;
}


int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    sleep(15);

    struct sockaddr_in dir_servidor, dir_cliente; //struct "ipv4, puerto, ip, relleno"
    unsigned int long_dir_cliente;
    int s2, proceso;
    char mensaje[1024], respuesta[1024];
    int recibidos;
    char *tipo_mensaje, *id_estacion, *ubicacion;

    if(argc != 3) {
        fprintf(stderr, "Uso: %s <PUERTO> <IP_KAFKA>\n", argv[0]);
        return 1;
    }

    char *servidor_puerto = argv[1];
    char *ip_kafka = argv[2];

    printf("Iniciando WM_Central...\n");
    printf("- Puerto Sockets: %s\n", servidor_puerto);
    printf("- Broker Kafka: %s\n", ip_kafka);

    s = socket(AF_INET, SOCK_STREAM, 0); //crea socket con ipv4 y tcp
    if(s == -1) {
        perror("Error al crear socket");
        return 1;
    }

    dir_servidor.sin_family = AF_INET; //ipv4
    dir_servidor.sin_port = htons(atoi(servidor_puerto)); //puerto
    dir_servidor.sin_addr.s_addr = INADDR_ANY; //dirección ip

    if(bind(s, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1) { //asocia socket con puerto
        perror("Error al asociar socket con puerto");
        close(s);
        return 1;
    }   

    if(listen(s, 5) == -1) { //escucha conexiones entrantes
        perror("Error al escuchar conexiones");
        close(s);
        return 1;
    }

    signal(SIGINT, finalizar); //(Ctrl+C)

    sqlite3 *db_init;
    if (sqlite3_open("water_management.db", &db_init) == SQLITE_OK) {
        const char *sql_create = "CREATE TABLE IF NOT EXISTS estaciones ("
                                 "id TEXT PRIMARY KEY, "
                                 "ubicacion TEXT, "
                                 "estado TEXT DEFAULT 'DISPONIBLE'); "
                                 "CREATE TABLE IF NOT EXISTS operarios ("
                                 "id TEXT PRIMARY KEY, "
                                 "nombre TEXT);";
        sqlite3_exec(db_init, sql_create, 0, 0, NULL);
        sqlite3_close(db_init);
        printf("[CENTRAL] Base de datos SQLite inicializada (Estaciones y Operarios listos).\n");
    }

    pthread_t hilo_kafka;
    pthread_create(&hilo_kafka, NULL, hilo_consumidor_kafka, (void *)ip_kafka);
    
    pthread_t hilo_menu;
    pthread_create(&hilo_menu, NULL, hilo_menu_central, (void *)ip_kafka);
    
    while(1){
        long_dir_cliente = sizeof(dir_cliente);
        s2 = accept(s, (struct sockaddr *)&dir_cliente, &long_dir_cliente); //acepta conexión entrante
        if(s2 == -1) { break; } 

        proceso = fork(); //crea un nuevo proceso para manejar la conexión
        if(proceso == -1) { perror("Error al crear proceso"); exit(1); }

        if(proceso == 0) { 
            close(s); //cierra socket principal en proceso hijo

            recibidos = read(s2, mensaje, sizeof(mensaje));
            if(recibidos > 0) {
                mensaje[recibidos] = '\0'; 
                mensaje[strcspn(mensaje, "\n")] = '\0'; 
                printf("Mensaje recibido [%d]: %s\n\r", recibidos, mensaje);

                //validacion del protocolo
                tipo_mensaje = strtok(mensaje, "#");
                id_estacion = strtok(NULL, "#");
                ubicacion = strtok(NULL, "#");

                if(tipo_mensaje != NULL && id_estacion != NULL && ubicacion != NULL){
                    if(strcmp(tipo_mensaje, "REGISTRO") == 0){

                        sqlite3 *db;
                        char *err_msg = 0;
                        char sql[512];
                        
                        if (sqlite3_open("water_management.db", &db) == SQLITE_OK) {

                            snprintf(sql, sizeof(sql), "INSERT INTO estaciones (id, ubicacion, estado) VALUES ('%s', '%s', 'DISPONIBLE');", id_estacion, ubicacion);
                            
                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) { // ejecutamos el insert
                                printf(">>> BD: Estacion %s en %s insertada con éxito <<<\n\r", id_estacion, ubicacion);
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

                    }else if(strcmp(tipo_mensaje, "AVERIA") == 0){
                        
                        sqlite3 *db;
                        char *err_msg = 0;
                        char sql[512];
                        
                        if (sqlite3_open("water_management.db", &db) == SQLITE_OK) {

                            snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'FUGA' WHERE id = '%s';", id_estacion);
                            
                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) { // ejecutamos el insert
                                printf(">>> BD: Estacion %s en %s reporta una FUGA <<<\n\r", id_estacion, ubicacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion actualizado a FUGA\n");
                            } else {
                                printf("Error BD: %s\n", err_msg);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al actualizar en BD\n");
                                sqlite3_free(err_msg);
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }

                    }else if(strcmp(tipo_mensaje, "REPARADO") == 0){

                        sqlite3 *db;
                        char *err_msg = 0;
                        char sql[512];
                        
                        if (sqlite3_open("water_management.db", &db) == SQLITE_OK) {

                            snprintf(sql, sizeof(sql), "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE id = '%s';", id_estacion);
                            
                            if (sqlite3_exec(db, sql, 0, 0, &err_msg) == SQLITE_OK) { // ejecutamos el insert
                                printf(">>> BD: Estacion %s en %s reporta que ha sido REPARADO <<<\n\r", id_estacion, ubicacion);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Estacion actualizado a DISPONIBLE\n");
                            } else {
                                printf("Error BD: %s\n", err_msg);
                                snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Fallo al actualizar en BD\n");
                                sqlite3_free(err_msg);
                            }
                            sqlite3_close(db);
                        } else {
                            snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Sin conexion a la BD\n");
                        }

                    }else{
                        snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Comando no reconocido\n");
                    }
                }else{
                    snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Formato de trama incorrecto\n");
                }
                
                
                write(s2, respuesta, strlen(respuesta));
            } 

            close(s2); 
            exit(0); //termina proceso hijo
        } else { 
            close(s2); 
        }
    }
    close(s);
    return 0;

}