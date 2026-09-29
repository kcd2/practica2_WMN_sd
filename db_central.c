#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>

int inicializar_base_datos() {
    sqlite3 *db;
    char *err_msg = NULL;
    int rc;

    rc = sqlite3_open("water_management.db", &db);

    if (rc != 0) { //if (rc != SQLITE_OK)
        fprintf(stderr, "No se pudo abrir la base de datos: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    } else {
        printf("Conexión con SQLite establecida correctamente.\n");
    }

    const char *sql_operarios = "CREATE TABLE IF NOT EXISTS operarios ("
                                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                "nombre TEXT NOT NULL, "
                                "rol TEXT NOT NULL);";

    const char *sql_estaciones = "CREATE TABLE IF NOT EXISTS estaciones ("
                                 "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                 "codigo_estacion TEXT UNIQUE NOT NULL, "
                                 "ubicacion TEXT, "
                                 "estado TEXT DEFAULT 'ACTIVA');";

    rc = sqlite3_exec(db, sql_operarios, 0, 0, &err_msg);
    if (rc != 0) {
        fprintf(stderr, "SQL error al crear tabla operarios: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_exec(db, sql_estaciones, 0, 0, &err_msg);
    if (rc != 0) {
        fprintf(stderr, "SQL error al crear tabla estaciones: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    printf("Tablas 'operarios' y 'estaciones' creadas.\n");

    sqlite3_close(db);
    return 0;
}

int main() {
    inicializar_base_datos();
    return 0;
}