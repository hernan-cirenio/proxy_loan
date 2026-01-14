# Métricas por CUIL/DNI (Creditech)

Este documento describe **variable por variable** la lógica actual de cálculo a partir de los archivos de entrada.

## Alcance (para QA)
- El objetivo es que Creditech pueda **validar resultados** (QA) entendiendo exactamente **qué se suma**, **qué se filtra** y **de qué campo sale cada valor**.
- No se incluyen datos reales de clientes: solo reglas de cálculo.

## Fuentes de datos

### `detalle.csv` (Detalle Cuotas)
Se lee con separador `;`. Las columnas relevantes son:
- **`DOCUMENTO`**: identificador del cliente (usado como CUIL/DNI).
- **`FECHA`**: fecha de alta/marca del crédito/registro (se usa para `fec_ult_prestamo`).
- **`FECHA VTO`**: vencimiento de la cuota.
- **`SALDO CTA`**: saldo pendiente de la cuota.
- **`IMPORTE CTA`**: importe de la cuota.
- **`F ULT PAGO`**: fecha del último pago registrado en el detalle.

Notas:
- Los importes se parsean con normalización de separadores decimales (coma/punto).
- Las fechas se parsean con formatos `dd/mm/YYYY` o `dd/mm/YY`. El valor `"01/01/0001"` se considera **nulo**.

### `convenios.csv` (Cartera / Convenios)
Se salta el encabezado “largo” hasta encontrar una línea que arranca con `NRO_ DOC`.
Las columnas relevantes son:
- **`NRO_ DOC`**: identificador del cliente (CUIL/DNI).
- **`DIAS_ATRASO`**: días de atraso informados por cartera (se usa en `dias_atraso_vigente`).
- **`NRO.CONVENIO`**: indica presencia de refinanciación.
- **`ESTADO OPERACION`**: se usa para excluir cancelados en el flag de refinanciación vigente.
- **`F.CONVENIO`**: fecha del convenio (se usa para “últimos 6 meses”).
- **`FECHA ULTIMO PAGO`**: **NO se usa** para `fec_ult_pago` (por definición del negocio).

## Identificación y agregación

- Se calcula por cliente usando el valor de **`DOCUMENTO`** (detalle) / **`NRO_ DOC`** (convenios) como **clave `cuil`**.
- El universo de clientes es la **unión** de CUILs presentes en ambas fuentes.
- Las métricas se agrupan por **mes calendario** usando `FECHA VTO` (año, mes).

## Variables calculadas (salida en `cuil_metrics`)

### `deuda_a_vencer_total_vigente`
**Definición**: “toda la deuda que le queda por vencer”.

**Cálculo** (desde `detalle.csv`):
- Para cada fila (cuota) del cliente:
  - Si `SALDO CTA > 0` **y** `FECHA VTO >= hoy`:
    - sumar `SALDO CTA` al acumulado.

**Notas**:
- Se considera “por vencer” todo saldo pendiente con vencimiento **hoy o futuro**.
- Si el cliente no aparece en detalle, el valor queda `0.0`.

---

### `suma_cuotas_prestamo_vigente`
**Definición**: “monto vigente a pagar / deuda vigente”.

**Cálculo** (desde `detalle.csv`):
- Por regla de negocio, **tiene exactamente el mismo valor que `deuda_a_vencer_total_vigente`**.

**Importante**:
- Se calcula con el mismo criterio de “deuda por vencer” (saldo pendiente con vencimiento hoy o futuro).

---

### `suma_cuotas_prestamo_mes_1`
**Definición**: “suma de las cuotas del próximo mes (mes actual +1)”.

**Cálculo** (desde `detalle.csv`):
- Igual que `suma_cuotas_prestamo_vigente`, pero tomando el bucket del mes **(hoy + 1 mes)**.

---

### `suma_cuotas_prestamo_mes_2`
**Definición**: “suma de las cuotas del mes posterior al próximo (mes actual +2)”.

**Cálculo** (desde `detalle.csv`):
- Igual que `suma_cuotas_prestamo_vigente`, pero tomando el bucket del mes **(hoy + 2 meses)**.

---

### `tiene_refinanciacion_vigente`
**Definición**: “si está presente en el documento de convenios”.

**Cálculo** (desde `convenios.csv`):
- Para el cliente, si existe al menos una fila con:
  - `NRO.CONVENIO` no vacío **y**
  - `ESTADO OPERACION` **no** contiene `"CANCELADO"` (case-insensitive)
- entonces `SI`, si no `NO`.

---

### `tiene_refinanciacion_ultimos_6_meses`
**Definición**: “si tiene alguna refinanciación en convenios de hace 6 meses o menos”.

**Cálculo** (desde `convenios.csv`):
- Para cada fila del cliente:
  - parsear `F.CONVENIO`
  - calcular diferencia en meses entre `hoy` y `F.CONVENIO`
  - si `delta_meses <= 6` ⇒ `SI`
- Si ninguna fila cumple ⇒ `NO`.

---

### `dias_atraso_vigente`
**Definición**: “días de atraso máximo de todos sus préstamos al momento de la consulta”.

**Cálculo** (combinado):
1) **Desde convenios**:
- `max(DIAS_ATRASO)` por cliente (si no parsea, se trata como 0).

2) **Desde detalle** (inferido):
- Para cada cuota con `SALDO CTA > 0`:
  - si `FECHA VTO < hoy`:
    - atraso = `hoy - FECHA VTO` (en días)
  - tomar el máximo por cliente.

**Resultado**:
- `dias_atraso_vigente = max(atraso_convenios, atraso_detalle)`

---

### `fec_ult_pago`
**Definición**: “fecha que realizó el último pago”.

**IMPORTANTE (regla de negocio)**: **solo se contempla `detalle.csv`**, no convenios.

**Cálculo** (desde `detalle.csv`):
- Para cada fila del cliente:
  - parsear `F ULT PAGO`
  - tomar el **máximo** (la fecha más reciente).

**Notas**:
- Si no existe ninguna fecha válida en detalle, queda `null`.

---

### `fec_ult_prestamo`
**Definición**: “fecha de alta del último préstamo”.

**Cálculo** (desde `detalle.csv`):
- Para cada fila del cliente:
  - parsear `FECHA`
  - tomar el **máximo** (la fecha más reciente).

**Por qué puede ser `null`**:
- Si el CUIL aparece en `convenios.csv` pero **no aparece en `detalle.csv`**, no hay de dónde inferir la fecha de alta del préstamo.
- Si la columna `FECHA` está vacía o no parsea en todas las filas, también queda `null`.

---

### `updated_at`
**Definición**: timestamp de actualización del cálculo.

**Cálculo**:
- Se setea al momento de procesar el job (UTC).

## Versionado por job (`job_id`)

Para soportar histórico y evitar borrar datos:
- `cuil_metrics` incluye **`job_id`**
- La PK es **(job_id, cuil)**, por lo que podés tener múltiples snapshots por cliente (uno por job).
- La API, al consultar, usa siempre el **último job con `status='completed'`**.

## Supuestos y limitaciones (para QA)
- Las métricas dependen de que `detalle.csv` contenga filas del cliente. Si el cliente solo está en `convenios.csv`, variables como `fec_ult_prestamo` pueden quedar `null`.
- Los buckets “mes actual / mes +1 / mes +2” se calculan por **mes calendario** (año/mes) usando `FECHA VTO`.
