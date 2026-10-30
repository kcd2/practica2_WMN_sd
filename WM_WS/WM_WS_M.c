#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/time.h>
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
int engine_conectado = 0;
int en_fuga = 0;        // 0 = DISPONIBLE, 1 = FUGA
int comando_engine = 0; // 0 = PING normal, 1 = DETENER_FUGA, 2 = RESTABLECER
int central_conectada = 0;
pthread_mutex_t mutex_estado_monitor = PTHREAD_MUTEX_INITIALIZER;

// cierra los sockets del Monitor cuando termina el proceso
void finalizar(int sig)
{
    printf("\n[MONITOR] Cerrando sockets y finalizando ejecucion...\n");
    if (s_engine_cliente != -1) {
        close(s_engine_cliente);
    }
    if (s_local_servidor != -1) {
        close(s_local_servidor);
    }
    exit(0);
}

// envia un mensaje a Central y recoge su respuesta
int enviar_trama_central(const char *mensaje, char *respuesta, int tam_respuesta)
{
    struct sockaddr_in direccion;
    struct hostent *he;
    int s;
    int n, enviados, recibidos;
    int es_latido = strncmp(mensaje, "LATIDO#", 7) == 0;

    // abrimos el socket
    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
    {
        if (!es_latido) {
            fprintf(stderr, "Error. No se puede abrir el socket\n");
        }
        return -1;
    }

    // resolver IP o Nombre de Contenedor (Docker DNS)
    he = gethostbyname(servidor_ip);
    if (he == NULL)
    {
        if (!es_latido) {
            fprintf(stderr, "Error. No se puede resolver el host/IP de Central: %s\n", servidor_ip);
        }
        close(s);
        return -1;
    }

    // conectar al servidor
    direccion.sin_family = AF_INET;
    direccion.sin_port = htons(atoi(servidor_puerto));
    memcpy(&direccion.sin_addr, he->h_addr_list[0], he->h_length);

    // timeout de 2 s para que si Central cae el Monitor no se quede bloqueado en connect/read
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == -1 ||
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == -1) {
        if (!es_latido) {
            fprintf(stderr, "Error configurando el tiempo de espera del socket\n");
        }
        close(s);
        pthread_mutex_lock(&mutex_estado_monitor);
        central_conectada = 0;
        pthread_mutex_unlock(&mutex_estado_monitor);
        return -1;
    }
    if (connect(s, (struct sockaddr *)&direccion, sizeof(direccion)) == -1) {
        if (!es_latido) {
            fprintf(stderr, "Error. No se puede conectar al servidor (%s:%s)\n", servidor_ip, servidor_puerto);
        }
        close(s);
        pthread_mutex_lock(&mutex_estado_monitor);
        central_conectada = 0;
        pthread_mutex_unlock(&mutex_estado_monitor);
        return -1;
    }

    // enviar mensaje
    n = strlen(mensaje);
    enviados = write(s, mensaje, n);
    if (enviados == -1 || enviados < n)
    {
        if (!es_latido) {
            fprintf(stderr, "Error enviando la trama a WM_Central\n");
        }
        pthread_mutex_lock(&mutex_estado_monitor);
        central_conectada = 0;
        pthread_mutex_unlock(&mutex_estado_monitor);
        close(s);
        return -1;
    }
    if (!es_latido) {
        printf("[MONITOR] Trama enviada a Central: %s", mensaje);
    }

    // recibir respuesta
    n = tam_respuesta - 1;
    recibidos = read(s, respuesta, n);

    if (recibidos == -1)
    {
        if (!es_latido) {
            fprintf(stderr, "Error recibiendo respuesta de WM_Central\n");
        }
        pthread_mutex_lock(&mutex_estado_monitor);
        central_conectada = 0;
        pthread_mutex_unlock(&mutex_estado_monitor);
        close(s);
        return -1;
    }
    respuesta[recibidos] = '\0';
    if (!es_latido) {
        printf("[MONITOR] Respuesta de WM_Central: %s\n", respuesta);
    }

    // validar que la respuesta comience por STATUS#OK
    if (strncmp(respuesta, "STATUS#OK", 9) != 0)
    {
        if (!es_latido) {
            fprintf(stderr, "[MONITOR] Error: La Central rechazo la trama o devolvio un estado no valido.\n");
        }
        pthread_mutex_lock(&mutex_estado_monitor);
        central_conectada = 0;
        pthread_mutex_unlock(&mutex_estado_monitor);
        close(s);
        return -1;
    }

    pthread_mutex_lock(&mutex_estado_monitor);
    // Central contestó OK: la marco conectada, el Engine lo sabe por el CENTRAL_OK/KO de cada ping
    central_conectada = 1;
    pthread_mutex_unlock(&mutex_estado_monitor);
    // cerrar el socket
    close(s);
    return 0;
}

// envia periódicamente el estado de la estación a Central
void *hilo_latido_central(void *arg)
{
    char mensaje[128];
    char respuesta[256];
    (void)arg;
    while (1)
    {
        int fuga_actual;
        int engine_activo;
        pthread_mutex_lock(&mutex_estado_monitor);
        fuga_actual = en_fuga;
        engine_activo = engine_conectado;
        pthread_mutex_unlock(&mutex_estado_monitor);

        // si hay fuga o el Engine no está conectado, el propio latido ya avisa de FUGA a Central
        const char *estado = "OK";
        if (fuga_actual || !engine_activo) {
            estado = "FUGA";
        }
        snprintf(mensaje, sizeof(mensaje), "LATIDO#%s#%s\n", id_estacion, estado);
        enviar_trama_central(mensaje, respuesta, sizeof(respuesta));
        sleep(1);
    }
    return NULL;
}


// acepta al Engine, comprueba su estado y avisa a Central
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
        // el Monitor hace de servidor: el Engine se conecta aquí y cada segundo le mando un ping
        tam_direccion = sizeof(direccion_cliente);
        s_engine_cliente = accept(s_local_servidor, (struct sockaddr *)&direccion_cliente, &tam_direccion);
        if (s_engine_cliente == -1)
        {
            continue;
        }

        printf("[MONITOR] WM_WS_E conectado. Iniciando comprobacion de estado de salud (1s)...\n");
        pthread_mutex_lock(&mutex_estado_monitor);
        engine_conectado = 1;
        pthread_mutex_unlock(&mutex_estado_monitor);

        while (1)
        {
            // ping normal por defecto; si hay un comando pendiente (avería/reparado manual) se lo mando en su lugar
            const char *consulta = "PING";
            int comando_actual;
            int central_actual;
            pthread_mutex_lock(&mutex_estado_monitor);
            comando_actual = comando_engine;
            comando_engine = 0;
            central_actual = central_conectada;
            pthread_mutex_unlock(&mutex_estado_monitor);

            if (comando_actual == 1)
            {
                consulta = "DETENER_FUGA";
            }
            else if (comando_actual == 2)
            {
                consulta = "RESTABLECER";
            }

            char mensaje_engine[128];
            const char *estado_central = "CENTRAL_KO";
            if (central_actual) {
                estado_central = "CENTRAL_OK";
            }
            snprintf(mensaje_engine, sizeof(mensaje_engine), "%s#%s\n", consulta, estado_central);
            enviados = write(s_engine_cliente, mensaje_engine, strlen(mensaje_engine));
            if (enviados <= 0)
            {
                fprintf(stderr, "[MONITOR] Error al enviar comprobacion a WM_WS_E (desconexion). Notificando AVERIA...\n");
                pthread_mutex_lock(&mutex_estado_monitor);
                engine_conectado = 0;
                en_fuga = 1;
                pthread_mutex_unlock(&mutex_estado_monitor);
                snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s#ENGINE_DESCONECTADO\n", id_estacion);
                enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central));
                close(s_engine_cliente);
                s_engine_cliente = -1;
                break;
            }

            recibidos = read(s_engine_cliente, buffer, sizeof(buffer) - 1);
            // si el Engine no responde o cierra la conexión lo doy por averiado y aviso a Central
            if (recibidos <= 0)
            {
                fprintf(stderr, "[MONITOR] Sin respuesta de WM_WS_E (desconexion). Notificando AVERIA...\n");
                pthread_mutex_lock(&mutex_estado_monitor);
                engine_conectado = 0;
                en_fuga = 1;
                pthread_mutex_unlock(&mutex_estado_monitor);
                snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s#ENGINE_DESCONECTADO\n", id_estacion);
                enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central));
                close(s_engine_cliente);
                s_engine_cliente = -1;
                break;
            }

            buffer[recibidos] = '\0';
            buffer[strcspn(buffer, "\r\n")] = '\0';

            int fuga_actual;
            pthread_mutex_lock(&mutex_estado_monitor);
            fuga_actual = en_fuga;
            pthread_mutex_unlock(&mutex_estado_monitor);

            // solo aviso en el cambio a fuga (!fuga_actual), no en cada ping, para no repetir la avería en Central
            if ((strcmp(buffer, "KO") == 0 || strcmp(buffer, "FUGA") == 0) && !fuga_actual)
            {
                printf("[MONITOR] Recibido %s desde WM_WS_E. Enviando AVERIA a WM_Central...\n", buffer);
                pthread_mutex_lock(&mutex_estado_monitor);
                en_fuga = 1;
                pthread_mutex_unlock(&mutex_estado_monitor);
                snprintf(mensaje_central, sizeof(mensaje_central), "AVERIA#%s#FUGA\n", id_estacion);
                enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central));
            }
            else if (strcmp(buffer, "OK") == 0 && fuga_actual)
            {
                // el Engine vuelve a OK: aviso REPARADO y solo limpio en_fuga si Central lo confirma
                printf("[MONITOR] Recibido OK desde WM_WS_E. Enviando REPARADO a WM_Central...\n");
                snprintf(mensaje_central, sizeof(mensaje_central), "REPARADO#%s\n", id_estacion);
                if (enviar_trama_central(mensaje_central, respuesta_central, sizeof(respuesta_central)) == 0)
                {
                    pthread_mutex_lock(&mutex_estado_monitor);
                    en_fuga = 0;
                    pthread_mutex_unlock(&mutex_estado_monitor);
                }
            }

            sleep(1);
        }
    }
    return NULL;
}

// registra el Monitor y arranca sus hilos de comunicación
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

    // Central puede tardar en arrancar, por eso reintento el registro hasta 10 veces cada 2 s
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
    pthread_t th_latido;
    // un hilo para los latidos a Central y otro para atender al Engine; el menú queda en el hilo principal
    pthread_create(&th_latido, NULL, hilo_latido_central, NULL);
    pthread_create(&th_engine, NULL, hilo_servidor_engine, NULL);

    char opcion[16];
    while (1)
    {
        printf("\n--- MENU MONITOR (%s - %s) ---\n", id_estacion, ubicacion);
        const char *estado_actual = "DISPONIBLE (OK)";
        int fuga_actual;
        pthread_mutex_lock(&mutex_estado_monitor);
        fuga_actual = en_fuga;
        pthread_mutex_unlock(&mutex_estado_monitor);
        if (fuga_actual) {
            estado_actual = "AVERIADA (FUGA)";
        }
        printf("Estado actual: %s\n", estado_actual);
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
                pthread_mutex_lock(&mutex_estado_monitor);
                en_fuga = 1;
                comando_engine = 1;
                pthread_mutex_unlock(&mutex_estado_monitor);
            }
        }
        else if (op == 2)
        {
            snprintf(mensaje, sizeof(mensaje), "REPARADO#%s\n", id_estacion);
            if (enviar_trama_central(mensaje, respuesta, sizeof(respuesta)) == 0)
            {
                pthread_mutex_lock(&mutex_estado_monitor);
                en_fuga = 0;
                comando_engine = 2;
                pthread_mutex_unlock(&mutex_estado_monitor);
            }
        }
    }

    return 0;
}
