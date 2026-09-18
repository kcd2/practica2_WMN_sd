#include <stdio.h>
#include <unistd.h> // para sleep

int main() {
    printf("el servidor ha arrancado bien en Railway\n");
    
    // más adelante la base de datos, los sockets y Kafka
    
    while(1) {
        sleep(10); 
    }

    return 0;
}