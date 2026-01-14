import csv
import os
import tempfile
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs

import boto3
import pymysql

DATABASE_URL = os.environ.get("DATABASE_URL")
DO_SPACES_ENDPOINT = os.environ.get("DO_SPACES_ENDPOINT")
DO_SPACES_BUCKET = os.environ.get("DO_SPACES_BUCKET")
DO_SPACES_KEY = os.environ.get("DO_SPACES_KEY")
DO_SPACES_SECRET = os.environ.get("DO_SPACES_SECRET")
DO_SPACES_REGION = os.environ.get("DO_SPACES_REGION", "us-east-1")
LOCAL_STORAGE = os.environ.get("LOCAL_STORAGE", "false").lower() == "true"
LOCAL_STORAGE_DIR = os.environ.get("LOCAL_STORAGE_DIR", "/data/uploads")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "20"))
RUN_ONCE = os.environ.get("RUN_ONCE", "false").lower() == "true"
REPROCESS_JOB_ID = os.environ.get("REPROCESS_JOB_ID")
REPROCESS_LATEST_COMPLETED = os.environ.get("REPROCESS_LATEST_COMPLETED", "false").lower() == "true"


def _get_mysql_ssl():
    mode = str(os.environ.get("DB_SSL_MODE", "REQUIRED")).upper()
    if mode == "DISABLED":
        return None

    ca_path = os.environ.get("DB_SSL_CA_PATH")
    ca_b64 = os.environ.get("DB_SSL_CA_BASE64")
    if ca_path and os.path.exists(ca_path):
        return {"ca": ca_path}

    if ca_b64:
        # Write CA to a temp file (PyMySQL expects a file path)
        import base64

        ca_bytes = base64.b64decode(ca_b64)
        tmp_path = os.path.join(tempfile.gettempdir(), "db-ca.pem")
        with open(tmp_path, "wb") as f:
            f.write(ca_bytes)
        return {"ca": tmp_path}

    # Fallback: TLS without CA verification (not ideal; prefer setting CA)
    return {"check_hostname": False}


def connect_mysql():
    if not DATABASE_URL:
        raise SystemExit("Missing DATABASE_URL for MySQL connection")

    parsed = urlparse(DATABASE_URL)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("mysql", "mysql2"):
        raise SystemExit("DATABASE_URL must start with mysql://")

    db_name = (parsed.path or "").lstrip("/")
    if not db_name:
        raise SystemExit("DATABASE_URL is missing database name")

    qs = parse_qs(parsed.query or "")
    charset = (qs.get("charset", ["utf8mb4"])[0]) or "utf8mb4"

    return pymysql.connect(
        host=parsed.hostname,
        user=parsed.username,
        password=parsed.password,
        database=db_name,
        port=parsed.port or 3306,
        autocommit=False,
        charset=charset,
        cursorclass=pymysql.cursors.DictCursor,
        ssl=_get_mysql_ssl(),
    )


def parse_decimal(value):
    if value is None:
        return 0.0
    value = value.strip()
    if not value:
        return 0.0
    if "," in value and "." in value:
        value = value.replace(".", "").replace(",", ".")
    elif "," in value:
        value = value.replace(",", ".")
    return float(value)


def parse_date(value):
    if value is None:
        return None
    value = value.strip()
    if not value or value == "01/01/0001":
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.date()
        except ValueError:
            continue
    return None


def month_key(dt_value):
    return dt_value.year, dt_value.month


def parse_convenios(path):
    results = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header = None
        for line in f:
            if line.startswith("NRO_ DOC"):
                header = next(csv.reader([line], delimiter=";"))
                break
        if not header:
            return results
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row:
                continue
            row = row + [""] * (len(header) - len(row))
            record = dict(zip(header, row))
            cuil = record.get("NRO_ DOC", "").strip().strip("'\"")
            if not cuil:
                continue
            data = results.setdefault(
                cuil,
                {
                    "dias_atraso": 0,
                    "fec_ult_pago": None,
                    "tiene_refi": "NO",
                    "tiene_refi_6m": "NO",
                },
            )

            dias = record.get("DIAS_ATRASO", "")
            try:
                dias_val = int(float(dias)) if dias else 0
            except ValueError:
                dias_val = 0
            data["dias_atraso"] = max(data["dias_atraso"], dias_val)

            pago = parse_date(record.get("FECHA ULTIMO PAGO", ""))
            if pago and (data["fec_ult_pago"] is None or pago > data["fec_ult_pago"]):
                data["fec_ult_pago"] = pago

            nro_convenio = record.get("NRO.CONVENIO", "").strip()
            estado = record.get("ESTADO OPERACION", "").upper()
            if nro_convenio and "CANCELADO" not in estado:
                data["tiene_refi"] = "SI"

            f_convenio = parse_date(record.get("F.CONVENIO", ""))
            if f_convenio:
                delta_months = (date.today().year - f_convenio.year) * 12 + (
                    date.today().month - f_convenio.month
                )
                if delta_months <= 6:
                    data["tiene_refi_6m"] = "SI"

    return results


def parse_detalle(path):
    aggregates = {}
    last_cuil = None
    today = date.today()
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader, [])
        if not header:
            return aggregates
        header_map = {name.strip(): idx for idx, name in enumerate(header)}

        idx_doc = header_map.get("DOCUMENTO")
        idx_vto = header_map.get("FECHA VTO")
        idx_saldo = header_map.get("SALDO CTA")
        idx_importe = header_map.get("IMPORTE CTA")
        idx_ult_pago = header_map.get("F ULT PAGO")
        idx_fecha_alta = header_map.get("FECHA")

        for row in reader:
            if not row:
                continue
            row = row + [""] * (len(header) - len(row))
            cuil = row[idx_doc].strip() if idx_doc is not None else ""
            if cuil:
                last_cuil = cuil
            else:
                cuil = last_cuil
            if not cuil:
                continue

            data = aggregates.setdefault(
                cuil,
                {
                    "deuda_a_vencer": 0.0,
                    "cuotas_mes": {},
                    "fec_ult_pago": None,
                    "fec_ult_prestamo": None,
                    "dias_atraso": 0,
                },
            )

            vto = parse_date(row[idx_vto] if idx_vto is not None else "")
            saldo = parse_decimal(row[idx_saldo] if idx_saldo is not None else "")
            importe = parse_decimal(row[idx_importe] if idx_importe is not None else "")

            if vto and saldo > 0:
                if vto >= today:
                    data["deuda_a_vencer"] += saldo
                data["cuotas_mes"].setdefault(month_key(vto), 0.0)
                data["cuotas_mes"][month_key(vto)] += importe
                if vto < today:
                    data["dias_atraso"] = max(data["dias_atraso"], (today - vto).days)

            ult_pago = parse_date(row[idx_ult_pago] if idx_ult_pago is not None else "")
            if ult_pago and (data["fec_ult_pago"] is None or ult_pago > data["fec_ult_pago"]):
                data["fec_ult_pago"] = ult_pago

            alta = parse_date(row[idx_fecha_alta] if idx_fecha_alta is not None else "")
            if alta and (data["fec_ult_prestamo"] is None or alta > data["fec_ult_prestamo"]):
                data["fec_ult_prestamo"] = alta

    return aggregates


def month_offset(base, offset):
    year = base.year + (base.month - 1 + offset) // 12
    month = (base.month - 1 + offset) % 12 + 1
    return year, month


def build_metrics(detalle_data, convenio_data):
    today = date.today()
    current_key = month_key(today)
    next_key = month_offset(today, 1)
    next2_key = month_offset(today, 2)
    metrics = {}

    all_cuils = set(detalle_data.keys()) | set(convenio_data.keys())

    for cuil in all_cuils:
        detalle = detalle_data.get(cuil, {})
        convenio = convenio_data.get(cuil, {})

        cuotas = detalle.get("cuotas_mes", {})
        # IMPORTANT: FEC_ULT_PAGO must come only from detalle cuotas (F ULT PAGO),
        # not from convenios.
        fec_ult_pago = detalle.get("fec_ult_pago")

        dias_atraso = max(detalle.get("dias_atraso", 0), convenio.get("dias_atraso", 0))

        metrics[cuil] = {
            "deuda_a_vencer_total_vigente": detalle.get("deuda_a_vencer", 0.0),
            # Business rule: must match deuda_a_vencer_total_vigente exactly
            "suma_cuotas_prestamo_vigente": detalle.get("deuda_a_vencer", 0.0),
            "suma_cuotas_prestamo_mes_1": cuotas.get(next_key, 0.0),
            "suma_cuotas_prestamo_mes_2": cuotas.get(next2_key, 0.0),
            "tiene_refinanciacion_vigente": convenio.get("tiene_refi", "NO"),
            "tiene_refinanciacion_ultimos_6_meses": convenio.get("tiene_refi_6m", "NO"),
            "dias_atraso_vigente": dias_atraso,
            "fec_ult_pago": fec_ult_pago.isoformat() if fec_ult_pago else None,
            "fec_ult_prestamo": detalle.get("fec_ult_prestamo").isoformat()
            if detalle.get("fec_ult_prestamo")
            else None,
        }

    return metrics


def process_job(conn, job):
    job_id, detalle_key, convenios_key = job
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET status = %s WHERE id = %s", ("processing", job_id))
    conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        detalle_path = os.path.join(tmpdir, "detalle.csv")
        convenios_path = os.path.join(tmpdir, "convenios.csv")

        if LOCAL_STORAGE:
            local_detalle = os.path.join(LOCAL_STORAGE_DIR, detalle_key)
            local_convenios = os.path.join(LOCAL_STORAGE_DIR, convenios_key)
            with open(local_detalle, "rb") as src, open(detalle_path, "wb") as dst:
                dst.write(src.read())
            with open(local_convenios, "rb") as src, open(convenios_path, "wb") as dst:
                dst.write(src.read())
        else:
            s3 = boto3.client(
                "s3",
                endpoint_url=DO_SPACES_ENDPOINT,
                aws_access_key_id=DO_SPACES_KEY,
                aws_secret_access_key=DO_SPACES_SECRET,
                region_name=DO_SPACES_REGION,
            )
            s3.download_file(DO_SPACES_BUCKET, detalle_key, detalle_path)
            s3.download_file(DO_SPACES_BUCKET, convenios_key, convenios_path)

        detalle_data = parse_detalle(detalle_path)
        convenio_data = parse_convenios(convenios_path)
        metrics = build_metrics(detalle_data, convenio_data)

        now = datetime.utcnow()
        rows = [
            (
                cuil,
                values.get("deuda_a_vencer_total_vigente", 0.0),
                values.get("suma_cuotas_prestamo_vigente", 0.0),
                values.get("suma_cuotas_prestamo_mes_1", 0.0),
                values.get("suma_cuotas_prestamo_mes_2", 0.0),
                values.get("tiene_refinanciacion_vigente", "NO"),
                values.get("tiene_refinanciacion_ultimos_6_meses", "NO"),
                values.get("dias_atraso_vigente", 0),
                values.get("fec_ult_pago"),
                values.get("fec_ult_prestamo"),
                now,
            )
            for cuil, values in metrics.items()
        ]

        insert_sql = (
            "INSERT INTO cuil_metrics (job_id, cuil, deuda_a_vencer_total_vigente, suma_cuotas_prestamo_vigente, "
            "suma_cuotas_prestamo_mes_1, suma_cuotas_prestamo_mes_2, "
            "tiene_refinanciacion_vigente, tiene_refinanciacion_ultimos_6_meses, "
            "dias_atraso_vigente, fec_ult_pago, fec_ult_prestamo, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "deuda_a_vencer_total_vigente=VALUES(deuda_a_vencer_total_vigente), "
            "suma_cuotas_prestamo_vigente=VALUES(suma_cuotas_prestamo_vigente), "
            "suma_cuotas_prestamo_mes_1=VALUES(suma_cuotas_prestamo_mes_1), "
            "suma_cuotas_prestamo_mes_2=VALUES(suma_cuotas_prestamo_mes_2), "
            "tiene_refinanciacion_vigente=VALUES(tiene_refinanciacion_vigente), "
            "tiene_refinanciacion_ultimos_6_meses=VALUES(tiene_refinanciacion_ultimos_6_meses), "
            "dias_atraso_vigente=VALUES(dias_atraso_vigente), "
            "fec_ult_pago=VALUES(fec_ult_pago), "
            "fec_ult_prestamo=VALUES(fec_ult_prestamo), "
            "updated_at=VALUES(updated_at)"
        )

        with conn.cursor() as cur:
            # Insert in chunks to avoid oversized packets
            chunk_size = int(os.environ.get("DB_INSERT_CHUNK", "2000"))
            for i in range(0, len(rows), chunk_size):
                payload = [(job_id,) + row for row in rows[i : i + chunk_size]]
                cur.executemany(insert_sql, payload)

            cur.execute(
                "UPDATE jobs SET status = %s, message = %s WHERE id = %s",
                ("completed", f"{len(metrics)} CUILs", job_id),
            )
        conn.commit()


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
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
            """
        )


def run_pending_jobs(conn):
    with conn.cursor() as cur:
        if REPROCESS_JOB_ID:
            cur.execute(
                "SELECT id, detalle_key, convenios_key FROM jobs WHERE id = %s",
                (int(REPROCESS_JOB_ID),),
            )
            jobs = cur.fetchall()
        elif REPROCESS_LATEST_COMPLETED:
            cur.execute(
                "SELECT id, detalle_key, convenios_key FROM jobs WHERE status = 'completed' ORDER BY id DESC LIMIT 1"
            )
            jobs = cur.fetchall()
        else:
            cur.execute("SELECT id, detalle_key, convenios_key FROM jobs WHERE status = 'uploaded' ORDER BY id")
            jobs = cur.fetchall()

    for job in jobs:
        try:
            process_job(conn, (job["id"], job["detalle_key"], job["convenios_key"]))
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status = %s, message = %s WHERE id = %s",
                    ("failed", str(exc), job["id"]),
                )
            conn.commit()

    return len(jobs)


def main():
    if not LOCAL_STORAGE and not all([DO_SPACES_ENDPOINT, DO_SPACES_BUCKET, DO_SPACES_KEY, DO_SPACES_SECRET]):
        raise SystemExit("Missing DO Spaces configuration")

    if RUN_ONCE:
        conn = connect_mysql()
        ensure_schema(conn)
        conn.commit()
        run_pending_jobs(conn)
        conn.close()
        return

    # Long-running worker loop (for App Platform "Worker" component)
    import time

    while True:
        try:
            conn = connect_mysql()
            ensure_schema(conn)
            conn.commit()
            processed = run_pending_jobs(conn)
            conn.close()

            # If we processed something, immediately check again; otherwise wait.
            if processed == 0:
                time.sleep(max(POLL_INTERVAL_SECONDS, 1))
        except Exception as exc:
            print(f"Worker loop error: {exc}")
            time.sleep(max(POLL_INTERVAL_SECONDS, 1))


if __name__ == "__main__":
    main()
