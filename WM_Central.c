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
    if(argc != 3) {
        fprintf(stderr, "Uso: %s <PUERTO> <IP_KAFKA>\n", argv[0]);
        exit(1);
    }

}