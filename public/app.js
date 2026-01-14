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
  const kv = (k, v) => `
    <div class="qa-kv">
      <div class="qa-kv__k">${escapeHtml(k)}</div>
      <div class="qa-kv__v">${escapeHtml(v)}</div>
    </div>
  `;

  const kvTop = [kv("CUIL", cuil), kv("Deuda a vencer (total vigente)", formatMoney(data.deuda_a_vencer_total_vigente))].join(
    ""
  );

  const cuotasRow = `
    <div class="qa-row qa-row--3">
      ${kv("Cuotas mes actual", formatMoney(data.suma_cuotas_prestamo_vigente))}
      ${kv("Cuotas mes +1", formatMoney(data.suma_cuotas_prestamo_mes_1))}
      ${kv("Cuotas mes +2", formatMoney(data.suma_cuotas_prestamo_mes_2))}
    </div>
  `;

  const kvRest = [
    kv("Refinanciación vigente", data.tiene_refinanciacion_vigente ?? "-"),
    kv("Refinanciación últimos 6 meses", data.tiene_refinanciacion_ultimos_6_meses ?? "-"),
    kv("Días atraso", data.dias_atraso_vigente ?? "-"),
    kv("Fec. último pago", data.fec_ult_pago ?? "-"),
    kv("Fec. último préstamo", data.fec_ult_prestamo ?? "-"),
    kv("Actualizado", data.updated_at ?? "-")
  ].join("");

  const raw = escapeHtml(JSON.stringify(data, null, 2));

  container.innerHTML = `
    <div class="qa-grid">
      ${kvTop}
    </div>
    ${cuotasRow}
    <div class="qa-grid">
      ${kvRest}
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

function setupUploadWithProgress() {
  const form = document.getElementById("upload-form");
  const progress = document.getElementById("upload-progress");
  const fill = document.getElementById("upload-progress-fill");
  const percentEl = document.getElementById("upload-progress-percent");
  const textEl = document.getElementById("upload-progress-text");
  const bar = progress?.querySelector('[role="progressbar"]');
  const logoutBtn = document.getElementById("logout-button");

  if (!form || !progress || !fill || !percentEl || !textEl || !bar) return;

  let uploading = false;

  function beforeUnloadHandler(e) {
    // Browser will show a generic confirmation dialog.
    e.preventDefault();
    e.returnValue = "";
  }

  const setUploading = (next) => {
    uploading = next;
    form.querySelectorAll("input, button").forEach((el) => (el.disabled = next));
    if (logoutBtn) logoutBtn.disabled = next;

    if (next) {
      window.addEventListener("beforeunload", beforeUnloadHandler);
    } else {
      window.removeEventListener("beforeunload", beforeUnloadHandler);
    }
  };

  const updateProgress = (pct, label) => {
    const clamped = Math.max(0, Math.min(100, Math.round(pct)));
    fill.style.width = `${clamped}%`;
    percentEl.textContent = `${clamped}%`;
    bar.setAttribute("aria-valuenow", String(clamped));
    if (label) textEl.textContent = label;
  };

  form.addEventListener("submit", (e) => {
    if (uploading) return;
    e.preventDefault();

    const fd = new FormData(form);
    progress.hidden = false;
    updateProgress(0, "Preparando carga…");
    setUploading(true);

    const xhr = new XMLHttpRequest();
    xhr.open("POST", form.getAttribute("action") || "/upload", true);
    xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
    xhr.setRequestHeader("Accept", "application/json");

    xhr.upload.onprogress = (ev) => {
      if (!ev.lengthComputable) {
        updateProgress(0, "Subiendo…");
        return;
      }
      const pct = (ev.loaded / ev.total) * 100;
      updateProgress(pct, "Subiendo archivos…");
    };

    xhr.onerror = () => {
      setUploading(false);
      updateProgress(0, "Error de red durante la carga");
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        updateProgress(100, "Carga completa. Procesando…");
        window.location.href = "/?message=ok";
        return;
      }

      let msg = `Error HTTP ${xhr.status}`;
      try {
        const parsed = JSON.parse(xhr.responseText || "{}");
        msg = parsed?.error || msg;
      } catch {
        // ignore
      }
      setUploading(false);
      updateProgress(0, msg);
    };

    xhr.send(fd);
  });
}

setupUploadWithProgress();
