#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <sqlite3.h>

int s; //socket

void finalizar(int senyal) {
    printf("\napagando servidor WM_Central...\n");
    close(s);
    exit(0);
}


int main(int argc, char *argv[]) {
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