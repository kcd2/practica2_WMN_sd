const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const db = require('./db');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8443;

function registrarAuditoria(origen, ip, accion, descripcion) {
  const fechaHora = new Date().toISOString().replace('T', ' ').substring(0, 19);
  db.run(
    'INSERT INTO auditoria (fecha_hora, origen, ip, accion, descripcion) VALUES (?, ?, ?, ?, ?)',
    [fechaHora, origen, ip || 'N/A', accion, descripcion]
  );
}

// 1. PUT /registry/estaciones/:id -> Alta de estación y generación de credencial
app.put('/registry/estaciones/:id', (req, res) => {
  const id = req.params.id;
  const ubicacion = req.body && req.body.ubicacion ? req.body.ubicacion.trim() : null;
  const ipCliente = req.socket.remoteAddress;

  if (!ubicacion) {
    return res.status(400).json({ error: 'Falta el campo ubicacion en el cuerpo JSON' });
  }

  db.get('SELECT * FROM credenciales WHERE id_ws = ? AND registrada = 1', [id], (err, row) => {
    if (err) return res.status(500).json({ error: 'Error interno de base de datos' });
    if (row) {
      registrarAuditoria(id, ipCliente, 'REGISTRO_DUPLICADO', `Intento de alta repetido para ${id}`);
      return res.status(409).json({ error: 'La estación ya está registrada' });
    }

    const credencial = crypto.randomBytes(16).toString('hex');
    const ahora = Math.floor(Date.now() / 1000);

    db.run(
      `INSERT OR REPLACE INTO credenciales (id_ws, ubicacion, token_registro, clave_aes, registrada, fecha_registro)
       VALUES (?, ?, ?, NULL, 1, ?)`,
      [id, ubicacion, credencial, ahora],
      (errInsert) => {
        if (errInsert) return res.status(500).json({ error: 'No se pudo guardar la credencial' });

        db.run(
          `INSERT OR IGNORE INTO estaciones (id, ubicacion, estado, caudal, volumen, estado_pre_desconexion, ultimo_latido)
           VALUES (?, ?, 'DESCONECTADA', 0.0, 0.0, 'DISPONIBLE', 0)`,
          [id, ubicacion]
        );

        registrarAuditoria(id, ipCliente, 'REGISTRO_OK', `Estación ${id} dada de alta en ${ubicacion}`);
        return res.status(201).json({
          id: id,
          ubicacion: ubicacion,
          credencial: credencial
        });
      }
    );
  });
});

// 2. GET /registry/estaciones/:id -> Consultar si está registrada
app.get('/registry/estaciones/:id', (req, res) => {
  const id = req.params.id;
  db.get('SELECT id_ws, ubicacion, registrada FROM credenciales WHERE id_ws = ? AND registrada = 1', [id], (err, row) => {
    if (err) return res.status(500).json({ error: 'Error interno de base de datos' });
    if (!row) return res.status(404).json({ error: 'Estación no registrada' });

    return res.status(200).json({
      id: row.id_ws,
      ubicacion: row.ubicacion,
      registrada: true
    });
  });
});

// 3. DELETE /registry/estaciones/:id -> Baja de estación
app.delete('/registry/estaciones/:id', (req, res) => {
  const id = req.params.id;
  const ipCliente = req.socket.remoteAddress;

  db.get('SELECT * FROM credenciales WHERE id_ws = ? AND registrada = 1', [id], (err, row) => {
    if (err) return res.status(500).json({ error: 'Error interno de base de datos' });
    if (!row) return res.status(404).json({ error: 'Estación no existe o ya estaba dada de baja' });

    db.run('DELETE FROM credenciales WHERE id_ws = ?', [id], (errDel) => {
      if (errDel) return res.status(500).json({ error: 'Error al eliminar credencial' });

      registrarAuditoria(id, ipCliente, 'BAJA_ESTACION', `Estación ${id} dada de baja en WM_Registry`);
      return res.status(200).json({ mensaje: `Baja hecha para ${id}` });
    });
  });
});

const opcionesHttps = {
  key: fs.readFileSync(path.join(__dirname, 'certs', 'key.pem')),
  cert: fs.readFileSync(path.join(__dirname, 'certs', 'cert.pem'))
};

https.createServer(opcionesHttps, app).listen(PORT, '0.0.0.0', () => {
  console.log(`[WM_Registry] Servidor HTTPS escuchando en https://0.0.0.0:${PORT}`);
});