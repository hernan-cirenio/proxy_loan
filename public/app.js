const fileInputs = document.querySelectorAll("input[type='file']");

fileInputs.forEach((input) => {
  input.addEventListener("change", () => {
    if (input.files.length > 0) {
      input.setAttribute("data-has-file", "true");
    } else {
      input.removeAttribute("data-has-file");
    }
  });
});

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeCuil(value) {
  return String(value ?? "").replace(/\D/g, "").trim();
}

function formatMoney(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function renderCuilResult(container, cuil, data) {
  const rows = [
    ["CUIL", cuil],
    ["Deuda a vencer (total vigente)", formatMoney(data.deuda_a_vencer_total_vigente)],
    ["Cuotas mes actual", formatMoney(data.suma_cuotas_prestamo_vigente)],
    ["Cuotas mes -1", formatMoney(data.suma_cuotas_prestamo_mes_1)],
    ["Cuotas mes -2", formatMoney(data.suma_cuotas_prestamo_mes_2)],
    ["Refinanciación vigente", data.tiene_refinanciacion_vigente ?? "-"],
    ["Refinanciación últimos 6 meses", data.tiene_refinanciacion_ultimos_6_meses ?? "-"],
    ["Días atraso", data.dias_atraso_vigente ?? "-"],
    ["Fec. último pago", data.fec_ult_pago ?? "-"],
    ["Fec. último préstamo", data.fec_ult_prestamo ?? "-"],
    ["Actualizado", data.updated_at ?? "-"]
  ];

  const kvHtml = rows
    .map(
      ([k, v]) => `
        <div class="qa-kv">
          <div class="qa-kv__k">${escapeHtml(k)}</div>
          <div class="qa-kv__v">${escapeHtml(v)}</div>
        </div>
      `
    )
    .join("");

  const raw = escapeHtml(JSON.stringify(data, null, 2));

  container.innerHTML = `
    <div class="qa-grid">
      ${kvHtml}
    </div>
    <details class="qa-raw">
      <summary>Ver JSON completo</summary>
      <pre><code>${raw}</code></pre>
    </details>
  `;
}

async function fetchCuil(cuil) {
  const res = await fetch(`/api/clientes/${encodeURIComponent(cuil)}`, {
    headers: { Accept: "application/json" }
  });

  if (res.status === 404) {
    return { ok: false, status: 404, error: "CUIL no encontrado" };
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    return { ok: false, status: res.status, error: text || `Error HTTP ${res.status}` };
  }

  const data = await res.json();
  return { ok: true, status: res.status, data };
}

function setupCuilQa() {
  const form = document.getElementById("cuil-qa-form");
  const input = document.getElementById("cuil-qa-input");
  const result = document.getElementById("cuil-qa-result");
  if (!form || !input || !result) return;

  const run = async (rawValue) => {
    const cuil = normalizeCuil(rawValue);
    if (!cuil) return;

    result.innerHTML = `<div class="qa-loading">Buscando CUIL <strong>${escapeHtml(cuil)}</strong>…</div>`;

    try {
      const r = await fetchCuil(cuil);
      if (!r.ok) {
        result.innerHTML = `<div class="alert">${escapeHtml(r.error)}</div>`;
        return;
      }

      renderCuilResult(result, cuil, r.data);
      const url = new URL(window.location.href);
      url.searchParams.set("cuil", cuil);
      window.history.replaceState({}, "", url.toString());
    } catch (err) {
      result.innerHTML = `<div class="alert">Error consultando el CUIL</div>`;
    }
  };

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    run(input.value);
  });

  const params = new URLSearchParams(window.location.search);
  const initial = params.get("cuil");
  if (initial) {
    input.value = initial;
    run(initial);
  }
}

setupCuilQa();
