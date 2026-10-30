# Prompts de apoyo al desarrollo

## Depuración de una incidencia: Parada de riego tras avería simulada

Tengo un problema con la simulación de fugas. En `WM_WS_E.c` pulso la tecla para simular el KO y en los logs veo que `WM_WS_M.c` detecta el fallo y manda `AVERIA#WS-01#FUGA` a Central por el socket TCP. 

El problema es que el riego no se detiene en el Engine: sigue soltando trazas de telemetría por Kafka hacia `wm_peticiones` y sumando litros en vez de abortar al momento. 

Revisa `WM_WS_E.c` y `WM_WS_M.c`. Dime exactamente por qué no se entera el hilo que gestiona el caudal (`hilo_ciclo_riego`) cuando el Monitor recibe el KO, qué variable o flag se nos está pasando actualizar.

## Consulta técnica: Dudas sobre el uso de librdkafka en C

Tengo varias dudas sobre cómo estructurar el código antes de ponerme a escribir:  (vamos a manejar tres topics: `wm_peticiones`, `wm_ordenes_ws` y `wm_respuestas_fo`).

1. ¿Cómo se configuran el productor y el consumidor con `rd_kafka_conf_set`? ¿Qué propiedades básicas son obligatorias y cómo compruebo si los parámetros son válidos?
2. En el consumidor, ¿cómo funciona el flujo de suscribirse a un topic (`rd_kafka_subscribe`) y leer mensajes con `rd_kafka_consumer_poll` sin que se quede colgado?
3. En el productor, veo que existe `rd_kafka_produce`, pero no entiendo bien para qué sirven `rd_kafka_flush` ni los callbacks de entrega (`dr_msg_cb`). ¿Realmente hacen falta si la función ya devuelve 0?

Ponme un ejemplo breve y sencillo que use alguno de los topics de la práctica para ver la sintaxis y qué errores típicos hay que capturar al llamar a estas funciones.




