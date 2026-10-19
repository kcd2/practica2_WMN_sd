// Archivo: WM_WS_M.c
// Compilación: gcc WM_WS_M.c -o WM_WS_M -lpthread

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

char *servidor_ip;
char *servidor_puerto;
char *puerto_local_engine;
char *id_estacion;
char *ubicacion;

int s_local_servidor = -1;
int s_engine_cliente = -1;
volatile int en_fuga = 0;        // 0 = DISPONIBLE, 1 = FUGA
volatile int comando_engine = 0; // 0 = PING normal, 1 = DETENER_FUGA, 2 = RESTABLECER

void finalizar(int sig)
{
    printf("\n[MONITOR] Cerrando sockets y finalizando ejecucion...\n");
    if (s_engine_cliente != -1) close(s_engine_cliente);
    if (s_local_servidor != -1) close(s_local_servidor);
    exit(0);
}

/* =========================================================================
 * FUNCION CLIENTE TCP CON SOPORTE DNS Y VALIDACION DE STATUS#OK
 * ========================================================================= */
int enviar_trama_central(const char *mensaje, char *respuesta, int tam_respuesta)
{
    struct sockaddr_in direccion;
    struct hostent *he;
    int s;
    int n, enviados, recibidos;

    /**** Paso 1: Abrir el socket ****/
    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
    {
        fprintf(stderr, "Error. No se puede abrir el socket\n");
        return -1;
    }

    /**** Resolver IP o Nombre de Contenedor (Docker DNS) ****/
    he = gethostbyname(servidor_ip);
    if (he == NULL)
    {
        fprintf(stderr, "Error. No se puede resolver el host/IP de Central: %s\n", servidor_ip);
        close(s);
        return -1;
    }

    /**** Conectar al servidor ****/
    direccion.sin_family = AF_INET;
    direccion.sin_port = htons(atoi(servidor_puerto));
    memcpy(&direccion.sin_addr, he->h_addr_list[0], he->h_length);

    if (connect(s, (struct sockaddr *)&direccion, sizeof(direccion)) == -1)
    {
        fprintf(stderr, "Error. No se puede conectar al servidor (%s:%s)\n", servidor_ip, servidor_puerto);
        close(s);
        return -1;
    }

    /**** Enviar mensaje ****/
    n = strlen(mensaje);
    enviados = write(s, mensaje, n);
    if (enviados == -1 || enviados < n)
    {
        fprintf(stderr, "Error enviando la trama a WM_Central\n");
        close(s);
        return -1;
    }
    printf("[MONITOR] Trama enviada a Central: %s", mensaje);

    /**** Recibir respuesta ****/
    n = tam_respuesta - 1;
    recibidos = read(s, respuesta, n);

    if (recibidos == -1)
    {
        fprintf(stderr, "Error recibiendo respuesta de WM_Central\n");
        close(s);
        return -1;
    }
    respuesta[recibidos] = '\0';
    printf("[MONITOR] Respuesta de WM_Central: %s\n", respuesta);

    /**** Validar que la respuesta comience por STATUS#OK ****/
    if (strncmp(respuesta, "STATUS#OK", 9) != 0)
    {
        fprintf(stderr, "[MONITOR] Error: La Central rechazo la trama o devolvio un estado no valido.\n");
        close(s);
        return -1;
    }

    /**** Cerrar el socket ****/
    close(s);
    return 0;
}

/* =========================================================================
 * HILO SERVIDOR LOCAL PARA COMPROBAR LA SALUD DE WM_WS_E CADA SEGUNDO
 * ========================================================================= */
void *hilo_servidor_engine(void *arg)
{
    struct sockaddr_in direccion_local, direccion_cliente;
    socklen_t tam_direccion;
    int opt = 1;
    char buffer[1024];
    char mensaje_central[1024];
    char respuesta_central[1024];
    int enviados, recibidos;

    s_local_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (s_local_servidor == -1)
    {
        fprintf(stderr, "Error. No se puede abrir el socket local para el Engine\n");
        exit(1);
    }

    setsockopt(s_local_servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    direccion_local.sin_family = AF_INET;
    direccion_local.sin_addr.s_addr = INADDR_ANY;
    direccion_local.sin_port = htons(atoi(puerto_local_engine));

    if (bind(s_local_servidor, (struct sockaddr *)&direccion_local, sizeof(direccion_local)) == -1)
    {
        fprintf(stderr, "Error en bind del puerto local %s para Engine\n", puerto_local_engine);
        close(s_local_servidor);
        exit(1);
    }

    if (listen(s_local_servidor, 5) == -1)
    {
        fprintf(stderr, "Error en listen del socket local\n");
        close(s_local_servidor);
        exit(1);
    }

    printf("[MONITOR] Servidor local abierto en puerto %s esperando a WM_WS_E...\n", puerto_local_engine);

    while (1)
    {
        tam_direccion = sizeof(direccion_cliente);
        s_engine_cliente = accept(s_local_servidor, (struct sockaddr *)&direccion_cliente, &tam_direccion);
        if (s_engine_cliente == -1)
        {
            continue;
        }

        printf("[MONITOR] WM_WS_E conectado. Iniciando comprobacion de estado de salud (1s)...\n");

        while (1)
        {
            const char *consulta = "PING\n";
            if (comando_engine == 1)
            {
                consulta = "DETENER_FUGA\n";
                comando_engine = 0;
            }
            else if (comando_engine == 2)
            {
                consulta = "RESTABLECER\n";
                comando_engine = 0;
            }

            enviados = write(s_engine_cliente, consulta, strlen(consulta));
            if (enviados <= 0)
            {
                fprintf(stderr, "[MONITOR] Error al enviar comprobacion a WM_WS_E (desconexion). Notificando AVERIA...\n");
                if (!en_fuga)
                {
                    snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s\n", id_estacion);
                    enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central));
                    en_fuga = 1;
                }
                close(s_engine_cliente);
                s_engine_cliente = -1;
                break;
            }

            recibidos = read(s_engine_cliente, buffer, sizeof(buffer) - 1);
            if (recibidos <= 0)
            {
                fprintf(stderr, "[MONITOR] Sin respuesta de WM_WS_E (desconexion). Notificando AVERIA...\n");
                if (!en_fuga)
                {
                    snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s\n", id_estacion);
                    enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central));
                    en_fuga = 1;
                }
                close(s_engine_cliente);
                s_engine_cliente = -1;
                break;
            }

            buffer[recibidos] = '\0';
            buffer[strcspn(buffer, "\r\n")] = '\0';

            if ((strcmp(buffer, "KO") == 0 || strcmp(buffer, "FUGA") == 0) && !en_fuga)
            {
                printf("[MONITOR] Recibido %s desde WM_WS_E. Enviando AVERIA a WM_Central...\n", buffer);
                snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s\n", id_estacion);
                if (enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central)) == 0)
                {
                    en_fuga = 1;
                }
            }
            else if (strcmp(buffer, "OK") == 0 && en_fuga)
            {
                printf("[MONITOR] Recibido OK desde WM_WS_E. Enviando REPARADO a WM_Central...\n");
                snprintf(mensaje_central, sizeof(mensaje_central), "REPARADO#%s\n", id_estacion);
                if (enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central)) == 0)
                {
                    en_fuga = 0;
                }
            }

            sleep(1);
        }
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    char mensaje[1024];
    char respuesta[1024];
    int intentos = 0;

    setbuf(stdout, NULL);
    signal(SIGINT, finalizar);

    if (argc != 6)
    {
        fprintf(stderr, "Error. Faltan parametros de conexion y registro.\n");
        fprintf(stderr, "Sintaxis: %s <ip_central> <puerto_central> <puerto_local_engine> <id_estacion> <ubicacion>\n", argv[0]);
        fprintf(stderr, "Ejemplo : %s 127.0.0.1 8080 9090 WS-01 \"River Park\"\n", argv[0]);
        return 1;
    }

    servidor_ip = argv[1];
    servidor_puerto = argv[2];
    puerto_local_engine = argv[3];
    id_estacion = argv[4];
    ubicacion = argv[5];

    snprintf(mensaje, sizeof(mensaje), "REGISTRO#%s#%s\n", id_estacion, ubicacion);
    printf("Intentando registrar [%s - %s] en %s:%s...\n", id_estacion, ubicacion, servidor_ip, servidor_puerto);

    while (intentos < 10)
    {
        if (enviar_trama_central(mensaje, respuesta, sizeof(respuesta)) == 0)
        {
            printf("Registro finalizado con exito.\n");
            break;
        }
        intentos++;
        printf("Reintentando conexion con WM_Central en 2 segundos (%d/10)...\n", intentos);
        sleep(2);
    }

    if (intentos == 10)
    {
        fprintf(stderr, "Error critico: No se pudo conectar con WM_Central tras 10 intentos.\n");
        return 1;
    }

    pthread_t th_engine;
    pthread_create(&th_engine, NULL, hilo_servidor_engine, NULL);

    char opcion[16];
    while (1)
    {
        printf("\n--- MENU MONITOR (%s - %s) ---\n", id_estacion, ubicacion);
        printf("Estado actual: %s\n", en_fuga ? "AVERIADA (FUGA)" : "DISPONIBLE (OK)");
        printf("1. Notificar AVERIA manualmente a WM_Central\n");
        printf("2. Notificar REPARADO manualmente a WM_Central\n");
        printf("Seleccione opcion: ");

        if (fgets(opcion, sizeof(opcion), stdin) == NULL)
        {
            sleep(1);
            continue;
        }

        int op = atoi(opcion);
        if (op == 1)
        {
            snprintf(mensaje, sizeof(mensaje), "AVERIA#%s\n", id_estacion);
            if (enviar_trama_central(mensaje, respuesta, sizeof(respuesta)) == 0)
            {
                en_fuga = 1;
                comando_engine = 1;
            }
        }
        else if (op == 2)
        {
            snprintf(mensaje, sizeof(mensaje), "REPARADO#%s\n", id_estacion);
            if (enviar_trama_central(mensaje, respuesta, sizeof(respuesta)) == 0)
            {
                en_fuga = 0;
                comando_engine = 2;
            }
        }
    }

    return 0;
}