import csv
import os
import sqlite3
import tempfile
from datetime import datetime, date

import boto3

DB_PATH = os.environ.get("DB_PATH", "./data/app.db")
DO_SPACES_ENDPOINT = os.environ.get("DO_SPACES_ENDPOINT")
DO_SPACES_BUCKET = os.environ.get("DO_SPACES_BUCKET")
DO_SPACES_KEY = os.environ.get("DO_SPACES_KEY")
DO_SPACES_SECRET = os.environ.get("DO_SPACES_SECRET")
DO_SPACES_REGION = os.environ.get("DO_SPACES_REGION", "us-east-1")
LOCAL_STORAGE = os.environ.get("LOCAL_STORAGE", "false").lower() == "true"
LOCAL_STORAGE_DIR = os.environ.get("LOCAL_STORAGE_DIR", "/data/uploads")


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
    prev_key = month_offset(today, -1)
    prev2_key = month_offset(today, -2)
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
            "suma_cuotas_prestamo_vigente": cuotas.get(current_key, 0.0),
            "suma_cuotas_prestamo_mes_1": cuotas.get(prev_key, 0.0),
            "suma_cuotas_prestamo_mes_2": cuotas.get(prev2_key, 0.0),
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
    conn.execute("UPDATE jobs SET status = ? WHERE id = ?", ("processing", job_id))
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

        conn.execute("DELETE FROM cuil_metrics")
        now = datetime.utcnow().isoformat()
        insert_sql = (
            "INSERT INTO cuil_metrics (cuil, deuda_a_vencer_total_vigente, suma_cuotas_prestamo_vigente, "
            "suma_cuotas_prestamo_mes_1, suma_cuotas_prestamo_mes_2, "
            "tiene_refinanciacion_vigente, tiene_refinanciacion_ultimos_6_meses, "
            "dias_atraso_vigente, fec_ult_pago, fec_ult_prestamo, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        for cuil, values in metrics.items():
            conn.execute(
                insert_sql,
                (
                    cuil,
                    values["deuda_a_vencer_total_vigente"],
                    values["suma_cuotas_prestamo_vigente"],
                    values["suma_cuotas_prestamo_mes_1"],
                    values["suma_cuotas_prestamo_mes_2"],
                    values["tiene_refinanciacion_vigente"],
                    values["tiene_refinanciacion_ultimos_6_meses"],
                    values["dias_atraso_vigente"],
                    values["fec_ult_pago"],
                    values["fec_ult_prestamo"],
                    now,
                ),
            )

        conn.execute(
            "UPDATE jobs SET status = ?, message = ? WHERE id = ?",
            ("completed", f"{len(metrics)} CUILs", job_id),
        )
        conn.commit()


def main():
    if not LOCAL_STORAGE and not all([DO_SPACES_ENDPOINT, DO_SPACES_BUCKET, DO_SPACES_KEY, DO_SPACES_SECRET]):
        raise SystemExit("Missing DO Spaces configuration")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    jobs = conn.execute(
        "SELECT id, detalle_key, convenios_key FROM jobs WHERE status = 'uploaded' ORDER BY id"
    ).fetchall()

    for job in jobs:
        try:
            process_job(conn, (job["id"], job["detalle_key"], job["convenios_key"]))
        except Exception as exc:
            conn.execute(
                "UPDATE jobs SET status = ?, message = ? WHERE id = ?",
                ("failed", str(exc), job["id"]),
            )
            conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
