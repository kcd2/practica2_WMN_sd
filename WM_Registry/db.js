const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Apunta a la misma BD compartida en la raíz del proyecto (../db_data/water_management.db)
const dbDir = path.join(__dirname, '..', 'db_data');
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

const dbPath = path.join(dbDir, 'water_management.db');
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  db.run('PRAGMA journal_mode = WAL;');
  db.run('PRAGMA busy_timeout = 3000;');

  // Tabla de estaciones (compatible con WM_Central.c de la Release 1)
  db.run(`
    CREATE TABLE IF NOT EXISTS estaciones (
      id TEXT PRIMARY KEY,
      ubicacion TEXT,
      estado TEXT DEFAULT 'DESCONECTADA',
      caudal REAL DEFAULT 0.0,
      volumen REAL DEFAULT 0.0,
      operario TEXT DEFAULT NULL,
      estado_pre_desconexion TEXT DEFAULT 'DISPONIBLE',
      ultimo_latido INTEGER DEFAULT 0,
      riego_inicio INTEGER DEFAULT NULL,
      riego_fin INTEGER DEFAULT NULL
    );
  `);

  // Tabla nueva de credenciales (#16)
  db.run(`
    CREATE TABLE IF NOT EXISTS credenciales (
      id_ws TEXT PRIMARY KEY,
      ubicacion TEXT NOT NULL,
      token_registro TEXT NOT NULL,
      clave_aes TEXT DEFAULT NULL,
      registrada INTEGER DEFAULT 1,
      fecha_registro INTEGER NOT NULL
    );
  `);

  // Tabla nueva de auditoría (#16)
  db.run(`
    CREATE TABLE IF NOT EXISTS auditoria (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha_hora TEXT NOT NULL,
      origen TEXT NOT NULL,
      ip TEXT DEFAULT 'N/A',
      accion TEXT NOT NULL,
      descripcion TEXT NOT NULL
    );
  `);
});

module.exports = db;