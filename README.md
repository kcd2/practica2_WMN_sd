# WaterManagement

Sistema distribuido para simular y supervisar una red de estaciones de riego. Está compuesto por una Central, aplicaciones de operario (FO) y dos procesos por estación: Monitor (WS_M) y Engine (WS_E). Central almacena el estado de la red en SQLite; los componentes intercambian peticiones y eventos mediante Apache Kafka y sockets TCP.

## Componentes

- `WM_Central/WM_Central.c`: registra y supervisa estaciones, procesa peticiones, mantiene la base de datos y sirve el panel web.
- `WM_FO/WM_FO.c`: aplicación de operario para consultar estaciones y solicitar riegos.
- `WM_WS/WM_WS_M.c`: monitor de estación; comunica su registro, latidos e incidencias a Central y comprueba el estado del Engine.
- `WM_WS/WM_WS_E.c`: ejecuta las órdenes de riego, genera telemetría y comunica el resultado del servicio.
- `dashboard/index.html`: panel de monitorización de Central.
- `config/estaciones.txt`: estaciones autorizadas, con formato `ID#Ubicación`.
- `config/operarios.txt`: operarios autorizados, con formato `ID#Nombre`.
- `prompts/prompts.md`: consultas de referencia relacionadas con el desarrollo y la depuración del sistema.

## Comunicaciones

Kafka se utiliza con estos tres topics:

- `wm_peticiones`: recibe solicitudes de FO y eventos del Engine, incluida la telemetría y el fin de riego.
- `wm_ordenes_ws`: distribuye órdenes de Central a los Engine.
- `wm_respuestas_fo`: entrega a FO las respuestas y los resultados procesados por Central.

Los topics contienen mensajes de texto con campos separados por `#`. La telemetría viaja como evento dentro de `wm_peticiones`; no existe un topic independiente llamado `wm_telemetria` en esta configuración.

Los sockets TCP cubren la comunicación entre Monitor y Central, y el enlace local entre Monitor y Engine. En el escenario Docker, Central escucha a los Monitor en el puerto `8080`; el panel HTTP de Central se publica en el puerto `8081`.

## Ejecución local

Desde la raíz del repositorio, Docker Compose construye y arranca los servicios:

```bash
docker compose up --build -d
```

El panel está disponible en [http://localhost:8081](http://localhost:8081). La API que utiliza el panel es `GET /api/status`. Para seguir los mensajes de Central:

```bash
docker compose logs -f central
```

El escenario se detiene con:

```bash
docker compose down
```

La configuración actual de Compose incluye cuatro pares Monitor/Engine (`WS-01`, `WS-02`, `WS-03` y `WS-08`) y dos procesos FO (`OP-01` y `OP-02`). La base de datos de Central se conserva en `db_data/`; FO y Engine usan volúmenes independientes para sus datos locales.

## Configuración

Central carga al inicio las identidades de `config/estaciones.txt` y `config/operarios.txt`. Los identificadores de estación y operario deben coincidir con los usados por los servicios de Compose. La lista `rutinas.txt` contiene también `WS-99`, que no figura entre las estaciones autorizadas y permite observar el rechazo de una solicitud.

Los ejecutables reciben sus direcciones y puertos mediante argumentos:

- Central: `WM_Central <puerto_tcp> <broker:puerto> [puerto_http]`.
- Monitor: `WM_WS_M <host_central> <puerto_central> <puerto_local_engine> <id_ws> <ubicación>`.
- Engine: `WM_WS_E <broker:puerto> <host_monitor> <puerto_monitor> <id_ws>`.
- FO: `WM_FO <broker:puerto> <id_operario> [fichero_rutinas]`.

Los nombres `broker`, `central` y los nombres de servicio de Compose resuelven dentro de la red Docker local. Para ejecutar componentes en equipos distintos, cada proceso necesita recibir la dirección de red alcanzable del servicio correspondiente.

## Persistencia local

Central mantiene en SQLite el estado de las estaciones y una cola de salida para los mensajes Kafka pendientes. Engine conserva eventos pendientes y el estado de un riego en sus datos locales; FO guarda la información necesaria para recuperar una operación pendiente. Estas persistencias permiten reintentar entregas después de reinicios. Un mensaje puede volver a publicarse si el proceso se interrumpe después de que Kafka lo acepte y antes de registrar la entrega localmente.

