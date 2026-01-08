import Database from "better-sqlite3";
import fs from "fs";
import path from "path";

const DB_PATH = process.env.DB_PATH || "./data/app.db";

export function getDb() {
  const dbDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at TEXT NOT NULL,
      status TEXT NOT NULL,
      message TEXT,
      detalle_key TEXT,
      convenios_key TEXT,
      detalle_name TEXT,
      convenios_name TEXT
    );

    CREATE TABLE IF NOT EXISTS cuil_metrics (
      cuil TEXT PRIMARY KEY,
      deuda_a_vencer_total_vigente REAL,
      suma_cuotas_prestamo_vigente REAL,
      suma_cuotas_prestamo_mes_1 REAL,
      suma_cuotas_prestamo_mes_2 REAL,
      tiene_refinanciacion_vigente TEXT,
      tiene_refinanciacion_ultimos_6_meses TEXT,
      dias_atraso_vigente INTEGER,
      fec_ult_pago TEXT,
      fec_ult_prestamo TEXT,
      updated_at TEXT NOT NULL
    );
  `);

  return db;
}
