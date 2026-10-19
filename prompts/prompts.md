# Prompts de apoyo al desarrollo

## Depuración de una incidencia

Estoy revisando el flujo entre `WM_Central`, `WM_WS_M`, `WM_WS_E` y `WM_FO`. Analiza los archivos relacionados con este problema: [describe aquí el síntoma]. Sigue el recorrido del mensaje desde que se genera hasta que se procesa, identifica la causa más probable con referencias a funciones o líneas y propón el cambio mínimo. Distingue lo que confirma el código de lo que habría que comprobar ejecutando el sistema. No cambies archivos hasta explicar el diagnóstico.

## Sintaxis de Kafka en C con librdkafka

Explícame cómo funciona en C la API de `librdkafka` que utiliza este proyecto. Usa como referencia los topics existentes `wm_peticiones`, `wm_ordenes_ws` y `wm_respuestas_fo`; no inventes topics nuevos. Quiero entender la configuración con `rd_kafka_conf_set`, la creación de productor y consumidor, la publicación con `rd_kafka_produce`, la suscripción con `rd_kafka_subscribe`, la lectura con `rd_kafka_consumer_poll`, y qué papel tienen `rd_kafka_flush`, los callbacks de entrega y la confirmación de offsets. Incluye un ejemplo pequeño compatible con el estilo de los archivos C del repositorio y explica los valores de retorno y los errores que deben comprobarse.

## Revisión de productor y consumidor

Revisa la configuración Kafka de [indica el componente o archivo]. Comprueba el broker, el topic, el `group.id`, la política de offsets y el orden de procesamiento. Explica si un mensaje podría perderse, duplicarse o quedar sin confirmar si el proceso se interrumpe en cada etapa. Basa las conclusiones en el código y propone cambios compatibles con la arquitectura del proyecto.

## Integración por sockets TCP

Analiza el intercambio TCP entre `WM_WS_M` y `WM_Central` para el mensaje [indica el mensaje]. Comprueba el formato separado por `#`, el delimitador de fin de trama, la lectura de respuestas y el tratamiento de errores de conexión. Mantén el enfoque de sockets BSD usado en la práctica y señala cualquier incompatibilidad entre emisor y receptor antes de proponer una corrección.

## Consulta y actualización de SQLite

Revisa esta operación de SQLite en `WM_Central`: [describe la operación]. Comprueba el esquema real, los parámetros enlazados, los códigos de retorno, el tratamiento de concurrencia y el cierre de sentencias y conexiones. Explica qué ocurre si la operación falla y propone una modificación pequeña que conserve el estilo actual del código.

## Recuperación de una operación del operario

Analiza cómo `WM_FO` guarda y recupera una solicitud pendiente al reiniciarse. Describe qué datos sobreviven al cierre, cómo se correlacionan las respuestas de Kafka con el operario y la estación, y qué sucede si FO, Central o la estación cae en distintos momentos. Propón una prueba reproducible y separa los resultados esperados de los que solo pueden confirmarse al ejecutar el escenario.

## Añadir una estación al escenario Docker

Quiero añadir una estación con ID [ID] y ubicación [ubicación] al escenario local. Indica qué entradas de configuración, servicios Monitor/Engine, puertos locales, argumentos y volúmenes deben revisarse. Verifica que los dos procesos usan el mismo ID, que el grupo Kafka del Engine queda asociado a esa estación y que la estación está autorizada por Central. No modifiques el repositorio sin mostrar primero la lista concreta de cambios.
