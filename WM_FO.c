#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int main(int argc, char **argv) {
    /*
    - Parámetros de entrada: Recibir por consola la IP/Puerto del broker 
    de Kafka y el ID del operario.   
    - Productor Kafka: Enviará peticiones de riego a la
    Central utilizando un topic (por ejemplo, wm_peticiones_riego).
    - Consumidor Kafka: Deberá escuchar las respuestas de la Central 
    para mostrar por pantalla si el riego ha sido autorizado, denegado, o el resumen final del mismo.  
    - Modo de ejecución: Debe permitir solicitar riegos de forma manual y 
    también de forma automática leyendo un archivo de texto, 
    esperando 4 segundos entre cada petición completada. 
    
    */

}