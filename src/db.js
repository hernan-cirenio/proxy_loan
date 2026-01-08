import fs from "fs";
import mysql from "mysql2/promise";

let pool;

function getSslConfig() {
  // DigitalOcean Managed MySQL requires TLS. Prefer providing the CA cert.
  // - DB_SSL_MODE=DISABLED to disable TLS for local dev
  // - DB_SSL_CA_PATH=/path/to/ca.pem (recommended)
  // - DB_SSL_CA_BASE64=... (alternative for Docker envs)
  const mode = String(process.env.DB_SSL_MODE || "REQUIRED").toUpperCase();
  if (mode === "DISABLED") return undefined;

  const caPath = process.env.DB_SSL_CA_PATH;
  const caBase64 = process.env.DB_SSL_CA_BASE64;
  const ca =
    (caPath && fs.existsSync(caPath) ? fs.readFileSync(caPath, "utf8") : null) ||
    (caBase64 ? Buffer.from(caBase64, "base64").toString("utf8") : null);

  // If no CA is provided, allow TLS but do not verify chain (not ideal; use CA if possible)
  if (!ca) {
    return { rejectUnauthorized: false };
  }

  return { ca, rejectUnauthorized: true };
}

export function getDb() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    throw new Error("Missing DATABASE_URL. Configure MySQL connection string in .env / App Platform env vars.");
  }
  if (dbUrl.includes("${")) {
    throw new Error(
      `Invalid DATABASE_URL (looks like an unresolved App Platform placeholder): ${dbUrl}. ` +
        "Set DATABASE_URL to the actual MySQL connection string (mysql://...) from the database component."
    );
  }

  if (!pool) {
    pool = mysql.createPool({
      uri: dbUrl,
      waitForConnections: true,
      connectionLimit: Number(process.env.DB_POOL_SIZE || 10),
      ssl: getSslConfig()
    });
  }
  return pool;
}

export async function ensureSchema(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      created_at DATETIME(6) NOT NULL,
      status VARCHAR(64) NOT NULL,
      message TEXT NULL,
      detalle_key TEXT NULL,
      convenios_key TEXT NULL,
      detalle_name TEXT NULL,
      convenios_name TEXT NULL,
      PRIMARY KEY (id),
      INDEX idx_jobs_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS cuil_metrics (
      job_id BIGINT UNSIGNED NOT NULL,
      cuil VARCHAR(32) NOT NULL,
      deuda_a_vencer_total_vigente DOUBLE NULL,
      suma_cuotas_prestamo_vigente DOUBLE NULL,
      suma_cuotas_prestamo_mes_1 DOUBLE NULL,
      suma_cuotas_prestamo_mes_2 DOUBLE NULL,
      tiene_refinanciacion_vigente VARCHAR(8) NULL,
      tiene_refinanciacion_ultimos_6_meses VARCHAR(8) NULL,
      dias_atraso_vigente INT NULL,
      fec_ult_pago DATE NULL,
      fec_ult_prestamo DATE NULL,
      updated_at DATETIME(6) NOT NULL,
      PRIMARY KEY (job_id, cuil),
      INDEX idx_metrics_cuil_job (cuil, job_id),
      INDEX idx_metrics_updated_at (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  // Lightweight migration for existing installs that had PRIMARY KEY(cuil)
  // and no job_id column. We attach legacy rows to job_id=0.
  const [colRows] = await db.query(
    `
      SELECT COUNT(*) AS cnt
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'cuil_metrics'
        AND COLUMN_NAME = 'job_id'
    `
  );
  const hasJobId = Number(colRows?.[0]?.cnt || 0) > 0;

  if (!hasJobId) {
    await db.query(`ALTER TABLE cuil_metrics ADD COLUMN job_id BIGINT UNSIGNED NULL FIRST`);
    await db.query(`UPDATE cuil_metrics SET job_id = 0 WHERE job_id IS NULL`);
    await db.query(`ALTER TABLE cuil_metrics MODIFY job_id BIGINT UNSIGNED NOT NULL`);
    await db.query(`ALTER TABLE cuil_metrics DROP PRIMARY KEY, ADD PRIMARY KEY (job_id, cuil)`);
    await db.query(`CREATE INDEX idx_metrics_cuil_job ON cuil_metrics (cuil, job_id)`);
  }
}
