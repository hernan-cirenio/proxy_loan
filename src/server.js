import express from "express";
import session from "express-session";
import multer from "multer";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { ensureSchema, getDb } from "./db.js";

const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || "change-me";
const APP_USER = process.env.APP_USER || "admin";
const APP_PASS = process.env.APP_PASS || "secret";
const API_KEY_CIRENIO = process.env.API_KEY_CIRENIO || "";
const API_KEY_BESMART = process.env.API_KEY_BESMART || "";

const DO_SPACES_ENDPOINT = process.env.DO_SPACES_ENDPOINT;
const DO_SPACES_BUCKET = process.env.DO_SPACES_BUCKET;
const DO_SPACES_KEY = process.env.DO_SPACES_KEY;
const DO_SPACES_SECRET = process.env.DO_SPACES_SECRET;
const DO_SPACES_REGION = process.env.DO_SPACES_REGION || "us-east-1";
const DO_SPACES_PREFIX = process.env.DO_SPACES_PREFIX || "";
const LOCAL_STORAGE = process.env.LOCAL_STORAGE === "true";
const LOCAL_STORAGE_DIR = process.env.LOCAL_STORAGE_DIR || "data/uploads";
const INLINE_PROCESS = process.env.INLINE_PROCESS === "true";
const PYTHON_BIN = process.env.PYTHON_BIN || "python3";

const app = express();
// Behind DigitalOcean App Platform / load balancers, trust proxy so req.ip uses X-Forwarded-For.
app.set("trust proxy", 1);
const db = getDb();
await ensureSchema(db);

const upload = multer({ dest: LOCAL_STORAGE_DIR });

function normalizeIdentifier(value) {
  return String(value ?? "").replace(/\D/g, "").trim();
}

function isValidIdentifier(value) {
  return /^[0-9]{1,11}$/.test(String(value ?? ""));
}

function requireApiAuth(req, res, next) {
  // Allow authenticated UI sessions to use /api/* without exposing the API key to the browser.
  if (req.session?.user === true) {
    return next();
  }

  const headerKey = req.get("x-api-key");
  const authHeader = req.get("authorization") || "";
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const provided = headerKey || bearer;

  const keys = [
    { name: "cirenio", value: API_KEY_CIRENIO },
    { name: "besmart", value: API_KEY_BESMART }
  ].filter((k) => k.value);

  if (keys.length === 0) {
    return res.status(401).json({ error: "API keys no configuradas" });
  }

  const match = provided ? keys.find((k) => k.value === provided) : null;
  if (!match) {
    return res.status(401).json({ error: "API key inválida" });
  }

  // Attach info for logging
  req.api_client = match.name;

  return next();
}

function requireCirenioApiKey(req, res, next) {
  if (req.api_client !== "cirenio") {
    return res.status(403).json({ error: "API key de Cirenio requerida" });
  }
  return next();
}

function getClientIp(req) {
  const fwd = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  return fwd || req.ip || null;
}

function apiRequestLogger(req, res, next) {
  // Only log requests authenticated via API key (not session UI)
  const apiClient = req.api_client;
  if (!apiClient) return next();

  const started = new Date();
  const ip = getClientIp(req);
  const userAgent = req.get("user-agent") || null;
  const path = req.originalUrl || req.url;
  const cuil = req.params?.cuil ? String(req.params.cuil) : null;

  res.on("finish", () => {
    // Fire and forget; do not block response.
    db.query(
      "INSERT INTO api_requests (created_at, api_client, ip, method, path, cuil, status_code, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [started, apiClient, ip, req.method, path, cuil, res.statusCode, userAgent]
    ).catch(() => {});
  });

  next();
}

function createS3Client() {
  if (!DO_SPACES_ENDPOINT || !DO_SPACES_BUCKET || !DO_SPACES_KEY || !DO_SPACES_SECRET) {
    throw new Error("Missing DO Spaces configuration");
  }

  return new S3Client({
    endpoint: DO_SPACES_ENDPOINT,
    region: DO_SPACES_REGION,
    credentials: {
      accessKeyId: DO_SPACES_KEY,
      secretAccessKey: DO_SPACES_SECRET
    }
  });
}

function renderPage(title, body) {
  return `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title}</title>
    <link rel="apple-touch-icon" sizes="180x180" href="https://www.cirenio.com/apple-touch-icon.png" />
    <link rel="icon" type="image/png" sizes="32x32" href="https://www.cirenio.com/favicon-32x32.png" />
    <link rel="icon" type="image/png" sizes="16x16" href="https://www.cirenio.com/favicon-16x16.png" />
    <link rel="shortcut icon" href="https://www.cirenio.com/favicon-32x32.png" />
    <link rel="stylesheet" href="/styles.css" />
  </head>
  <body>
    <div class="bg-orb orb-1"></div>
    <div class="bg-orb orb-2"></div>
    <header class="app-header">
      <div class="app-header__inner">
        <div class="brand">
          <img
            class="brand__logo brand__logo--cirenio"
            src="https://www.cirenio.com/images/logo-cirenio-white.svg"
            alt="Cirenio"
            decoding="async"
            loading="eager"
          />
          <span class="brand__divider" aria-hidden="true"></span>
          <img
            class="brand__logo brand__logo--client"
            src="https://creditech.com.ar/wp-content/uploads/2025/06/logo-footer.png"
            alt="Creditech"
            decoding="async"
            loading="eager"
          />
        </div>
      </div>
    </header>
    <main class="page">
      ${body}
    </main>
    <script src="/app.js"></script>
  </body>
</html>`;
}

function renderLogin(error) {
  return renderPage(
    "Ingreso | Loan Proxy",
    `
    <section class="card card--narrow">
      <header>
        <p class="eyebrow">Loan Proxy</p>
        <h1>Ingreso seguro</h1>
        <p class="muted">Accedé al panel de cargas para procesar los archivos.</p>
      </header>
      ${error ? `<div class="alert">${error}</div>` : ""}
      <form class="form" method="post" action="/login">
        <label>
          Usuario
          <input name="username" autocomplete="username" required />
        </label>
        <label>
          Contraseña
          <input name="password" type="password" autocomplete="current-password" required />
        </label>
        <button class="primary" type="submit">Ingresar</button>
      </form>
    </section>
    `
  );
}

function renderDashboard({ jobs, message }) {
  const rows = jobs
    .map(
      (job) => `
        <tr>
          <td>#${job.id}</td>
          <td>${job.status}</td>
          <td>${job.created_at}</td>
          <td>${job.detalle_name || "-"}</td>
          <td>${job.convenios_name || "-"}</td>
        </tr>
      `
    )
    .join("");

  return renderPage(
    "Cargas | Loan Proxy",
    `
    <section class="hero">
      <div>
        <p class="eyebrow">Panel de cargas</p>
        <h1>Importador de archivos</h1>
        <p class="muted">Subí ambos CSV y el job se procesará de forma asincrónica.</p>
      </div>
      <form method="post" action="/logout">
        <button class="ghost" type="submit" id="logout-button">Salir</button>
      </form>
    </section>

    ${message ? `<div class="alert success">${message}</div>` : ""}

    <section class="card">
      <h2>Nuevo procesamiento</h2>
      <form class="form grid" id="upload-form" action="/upload" method="post" enctype="multipart/form-data">
        <label>
          Detalle Cuotas
          <input type="file" name="detalle" accept=".csv" required />
        </label>
        <label>
          Convenios
          <input type="file" name="convenios" accept=".csv" required />
        </label>
        <button class="primary" type="submit">Subir archivos</button>
      </form>
      <div id="upload-progress" class="upload-progress" hidden>
        <div class="upload-progress__meta" aria-live="polite">
          <span id="upload-progress-text">Subiendo…</span>
          <span id="upload-progress-percent">0%</span>
        </div>
        <div class="upload-progress__bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
          <div id="upload-progress-fill" class="upload-progress__fill" style="width: 0%"></div>
        </div>
        <div id="upload-progress-hint" class="muted upload-progress__hint">
          No cierres esta pestaña ni navegues hacia atrás mientras dura la carga.
        </div>
      </div>
    </section>

    <section class="card">
      <h2>Buscar datos por CUIL</h2>
      <p class="muted">Buscá un CUIL/DNI y revisá los valores calculados.</p>
      <form class="form grid" id="cuil-qa-form">
        <label>
          CUIL/DNI
          <input
            id="cuil-qa-input"
            name="cuil"
            inputmode="numeric"
            autocomplete="off"
            placeholder="Ej: 2036732332"
            required
          />
        </label>
        <button class="primary" type="submit">Buscar</button>
      </form>
      <div id="cuil-qa-result" class="qa-result" aria-live="polite"></div>
    </section>

    <section class="card">
      <h2>Últimos procesamientos</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Estado</th>
              <th>Creado</th>
              <th>Detalle</th>
              <th>Convenios</th>
            </tr>
          </thead>
          <tbody>
            ${rows || `<tr><td colspan="5" class="muted">Sin jobs todavía</td></tr>`}
          </tbody>
        </table>
      </div>
    </section>
    `
  );
}

function runInlineWorker() {
  const child = spawn(PYTHON_BIN, ["worker/importer.py"], {
    stdio: "inherit",
    env: process.env
  });

  child.on("error", (error) => {
    console.error("No se pudo ejecutar el worker inline:", error.message);
  });
}

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      maxAge: 1000 * 60 * 60 * 8
    }
  })
);

app.use(express.static("public"));

function requireAuth(req, res, next) {
  if (req.session?.user === true) {
    return next();
  }
  return res.redirect("/login");
}

// Protect all API endpoints via session or API key.
app.use("/api", requireApiAuth);
app.use("/api", apiRequestLogger);

app.get("/login", (req, res) => {
  if (req.session?.user === true) {
    return res.redirect("/");
  }
  return res.send(renderLogin());
});

app.post("/login", (req, res) => {
  const { username, password } = req.body;
  if (username === APP_USER && password === APP_PASS) {
    req.session.user = true;
    return res.redirect("/");
  }
  return res.status(401).send(renderLogin("Credenciales inválidas"));
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

app.get("/", requireAuth, (req, res) => {
  (async () => {
    const [rows] = await db.query("SELECT * FROM jobs ORDER BY id DESC LIMIT 5");
    const jobs = rows.map((r) => ({
      ...r,
      created_at: r.created_at ? new Date(r.created_at).toISOString() : null
    }));
    const message = req.query.message === "ok" ? "Archivos recibidos. El job queda en cola." : null;
    res.send(renderDashboard({ jobs, message }));
  })().catch((err) => {
    console.error(err);
    res.status(500).send("Error cargando dashboard");
  });
});

app.post(
  "/upload",
  requireAuth,
  upload.fields([
    { name: "detalle", maxCount: 1 },
    { name: "convenios", maxCount: 1 }
  ]),
  async (req, res) => {
    const wantsJson = Boolean(req.xhr) || req.accepts(["json", "html"]) === "json";
    const detalleFile = req.files?.detalle?.[0];
    const conveniosFile = req.files?.convenios?.[0];

    if (!detalleFile || !conveniosFile) {
      if (wantsJson) {
        return res.status(400).json({ ok: false, error: "Faltan archivos" });
      }
      const [rows] = await db.query("SELECT * FROM jobs ORDER BY id DESC LIMIT 5");
      const jobs = rows.map((r) => ({
        ...r,
        created_at: r.created_at ? new Date(r.created_at).toISOString() : null
      }));
      return res.status(400).send(renderDashboard({ jobs, message: "Faltan archivos" }));
    }

    const now = new Date();
    const [insertRes] = await db.query(
      "INSERT INTO jobs (created_at, status, detalle_name, convenios_name) VALUES (?, ?, ?, ?)",
      [now, "pending_upload", detalleFile.originalname, conveniosFile.originalname]
    );
    const jobId = insertRes.insertId;

    try {
      const prefix = String(DO_SPACES_PREFIX || "")
        .replace(/\\/g, "/")
        .replace(/^\/+|\/+$/g, "")
        .split("/")
        .filter((p) => p && p !== "." && p !== "..")
        .join("/");
      const keyBase = prefix ? `${prefix}/` : "";

      if (LOCAL_STORAGE) {
        const detalleKey = path.join(keyBase, "imports", String(jobId), "detalle.csv");
        const conveniosKey = path.join(keyBase, "imports", String(jobId), "convenios.csv");
        const detalleDest = path.join(LOCAL_STORAGE_DIR, detalleKey);
        const conveniosDest = path.join(LOCAL_STORAGE_DIR, conveniosKey);

        fs.mkdirSync(path.dirname(detalleDest), { recursive: true });
        fs.mkdirSync(path.dirname(conveniosDest), { recursive: true });

        fs.renameSync(detalleFile.path, detalleDest);
        fs.renameSync(conveniosFile.path, conveniosDest);

        await db.query("UPDATE jobs SET status = ?, detalle_key = ?, convenios_key = ? WHERE id = ?", [
          "uploaded",
          detalleKey,
          conveniosKey,
          jobId
        ]);
      } else {
        const s3 = createS3Client();
        const detalleKey = `${keyBase}imports/${jobId}/detalle.csv`;
        const conveniosKey = `${keyBase}imports/${jobId}/convenios.csv`;

        await s3.send(
          new PutObjectCommand({
            Bucket: DO_SPACES_BUCKET,
            Key: detalleKey,
            Body: fs.createReadStream(detalleFile.path),
            ContentType: detalleFile.mimetype
          })
        );
        await s3.send(
          new PutObjectCommand({
            Bucket: DO_SPACES_BUCKET,
            Key: conveniosKey,
            Body: fs.createReadStream(conveniosFile.path),
            ContentType: conveniosFile.mimetype
          })
        );

        await db.query("UPDATE jobs SET status = ?, detalle_key = ?, convenios_key = ? WHERE id = ?", [
          "uploaded",
          detalleKey,
          conveniosKey,
          jobId
        ]);
      }

      if (INLINE_PROCESS) {
        runInlineWorker();
      }

      if (wantsJson) {
        return res.json({ ok: true, jobId });
      }
      return res.redirect("/?message=ok");
    } catch (error) {
      await db.query("UPDATE jobs SET status = ?, message = ? WHERE id = ?", ["failed", error.message, jobId]);
      if (wantsJson) {
        return res.status(500).json({ ok: false, error: error.message || "Error subiendo archivos" });
      }
      const [rows] = await db.query("SELECT * FROM jobs ORDER BY id DESC LIMIT 5");
      const jobs = rows.map((r) => ({
        ...r,
        created_at: r.created_at ? new Date(r.created_at).toISOString() : null
      }));
      return res.status(500).send(renderDashboard({ jobs, message: "Error subiendo archivos" }));
    } finally {
      if (!LOCAL_STORAGE) {
        fs.unlink(detalleFile.path, () => {});
        fs.unlink(conveniosFile.path, () => {});
      }
    }
  }
);

app.get("/api/loan/clients/:cuil", async (req, res) => {
  return handleClienteMetricsRequest(req, res, { source: "proxy" });
});

async function fetchCoreApiDataByUid(customerUid, requestedIdentifier, person) {
  const apiHost = process.env.LOANS_API_HOST;
  const apiKey = process.env.LOANS_API_KEY;

  if (!apiHost || !apiKey) {
    console.error("[fetchCoreApiDataByUid] Configuración faltante:", { hasHost: !!apiHost, hasKey: !!apiKey });
    throw new Error("INTERNAL_ERROR");
  }

  const url = `${apiHost}/api/v3/customers/${encodeURIComponent(customerUid)}/history`;
  console.log(`[fetchCoreApiDataByUid] Iniciando llamada para uid: ${customerUid}`);
  console.log(`[fetchCoreApiDataByUid] URL: ${url}`);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    console.warn(`[fetchCoreApiDataByUid] Timeout después de 10 segundos para uid: ${customerUid}`);
    controller.abort();
  }, 10000);

  try {
    console.log("[fetchCoreApiDataByUid] Ejecutando fetch...");
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-API-KEY": apiKey,
        "Content-Type": "application/json"
      },
      signal: controller.signal
    });

    clearTimeout(timeoutId);
    console.log(`[fetchCoreApiDataByUid] Respuesta recibida. Status: ${response.status} ${response.statusText}`);

    if (!response.ok) {
      const errorText = await response.text().catch(() => "No se pudo leer el cuerpo de la respuesta");
      console.error(
        `[fetchCoreApiDataByUid] Respuesta no OK. Status: ${response.status}, Body: ${errorText.substring(0, 200)}`
      );

      if (response.status === 404) {
        throw new Error("CUIL no encontrado");
      }
      throw new Error("INTERNAL_ERROR");
    }

    console.log("[fetchCoreApiDataByUid] Parseando respuesta JSON...");
    const apiResponse = await response.json().catch((parseError) => {
      console.error("[fetchCoreApiDataByUid] Error al parsear JSON:", parseError.message);
      throw new Error("INTERNAL_ERROR");
    });

    console.log(
      `[fetchCoreApiDataByUid] JSON parseado exitosamente. Code: ${apiResponse.code}, Message: ${apiResponse.message || "N/A"}`
    );

    if (!apiResponse.data) {
      console.error("[fetchCoreApiDataByUid] Respuesta inválida: falta campo 'data'", { keys: Object.keys(apiResponse) });
      throw new Error("INTERNAL_ERROR");
    }

    const d = apiResponse.data;
    console.log(
      `[fetchCoreApiDataByUid] Datos extraídos exitosamente. CUIL: ${
        d.customer?.identification?.cuil || person?.cuil || requestedIdentifier
      }`
    );

    const mappedData = {
      cuil: d.customer?.identification?.cuil || person?.cuil || requestedIdentifier,
      deuda_a_vencer_total_vigente: d.active_loans_data?.total_due_debt_active ?? d.summary?.financial?.total_debt ?? null,
      suma_cuotas_prestamo_vigente: d.active_loans_data?.active_loans_installments_sum ?? null,
      suma_cuotas_prestamo_mes_1: d.active_loans_data?.month_1_installments_sum ?? null,
      suma_cuotas_prestamo_mes_2: d.active_loans_data?.month_2_installments_sum ?? null,
      tiene_refinanciacion_vigente: d.refinancing?.has_active_refinancing ? "SI" : "NO",
      tiene_refinanciacion_ultimos_6_meses: d.refinancing?.has_refinancing_last_6_months ? "SI" : "NO",
      dias_atraso_vigente: d.default_info?.days_in_default ?? null,
      fec_ult_pago: d.default_info?.last_payment_date || null,
      fec_ult_prestamo: d.last_loan_date || null,
      updated_at: d.updated_at ? new Date(d.updated_at).toISOString() : new Date().toISOString(),
      source: "core"
    };

    console.log(`[fetchCoreApiDataByUid] Mapeo completado exitosamente para uid: ${customerUid}`);
    return mappedData;
  } catch (error) {
    clearTimeout(timeoutId);

    console.error(`[fetchCoreApiDataByUid] Error capturado para uid: ${customerUid}`);
    console.error(`[fetchCoreApiDataByUid] Error name: ${error.name}`);
    console.error(`[fetchCoreApiDataByUid] Error message: ${error.message}`);
    console.error(`[fetchCoreApiDataByUid] Error code: ${error.code || "N/A"}`);

    if (error.cause) {
      console.error("[fetchCoreApiDataByUid] Error cause:", error.cause);
    }

    if (error.stack) {
      console.error("[fetchCoreApiDataByUid] Stack trace:", error.stack);
    }

    if (error.message === "CUIL no encontrado") {
      throw error;
    }

    throw new Error("INTERNAL_ERROR");
  }
}

async function resolveCirenioPerson(identifier) {
  const [byCuil] = await db.query(
    "SELECT uid, cuil, dni, gender, created_at, updated_at FROM cirenio_persons WHERE cuil = ? LIMIT 2",
    [identifier]
  );

  if (byCuil.length > 1) {
    const error = new Error("MULTIPLE_PERSON_MATCHES");
    error.statusCode = 409;
    throw error;
  }

  if (byCuil.length === 1) {
    return { person: byCuil[0], matchedBy: "cuil" };
  }

  const [byDni] = await db.query(
    "SELECT uid, cuil, dni, gender, created_at, updated_at FROM cirenio_persons WHERE dni = ? LIMIT 2",
    [identifier]
  );

  if (byDni.length > 1) {
    const error = new Error("MULTIPLE_PERSON_MATCHES");
    error.statusCode = 409;
    throw error;
  }

  if (byDni.length === 1) {
    return { person: byDni[0], matchedBy: "dni" };
  }

  return null;
}

app.post("/api/core/persons", requireCirenioApiKey, async (req, res) => {
  const cuil = normalizeIdentifier(req.body?.cuil);
  const dni = normalizeIdentifier(req.body?.dni);
  const uid = String(req.body?.uid || "").trim();
  const rawGender = String(req.body?.gender || req.body?.genero || "").trim().toLowerCase();
  const gender = rawGender || null;

  if (!uid || !cuil || !dni) {
    return res.status(400).json({ error: "Faltan campos requeridos: cuil, dni, uid" });
  }

  if (!/^[0-9]{11}$/.test(cuil)) {
    return res.status(400).json({ error: "CUIL inválido" });
  }

  if (!isValidIdentifier(dni)) {
    return res.status(400).json({ error: "DNI inválido" });
  }

  if (uid.length !== 8) {
    return res.status(400).json({ error: "UID inválido" });
  }

  if (gender && !["m", "f", "x"].includes(gender)) {
    return res.status(400).json({ error: "Gender inválido" });
  }

  let conn;
  try {
    conn = await db.getConnection();
    await conn.beginTransaction();

    const [uidRows] = await conn.query("SELECT uid, cuil, dni FROM cirenio_persons WHERE uid = ? LIMIT 1", [uid]);
    const existingByUid = uidRows[0] || null;

    const [cuilRows] = await conn.query("SELECT uid, cuil, dni FROM cirenio_persons WHERE cuil = ? LIMIT 1", [cuil]);
    const existingByCuil = cuilRows[0] || null;

    if (existingByUid && existingByUid.cuil && existingByUid.cuil !== cuil) {
      await conn.rollback();
      return res.status(409).json({ error: "UID ya asociado a otro CUIL" });
    }

    if (existingByCuil && existingByCuil.uid !== uid) {
      await conn.rollback();
      return res.status(409).json({ error: "CUIL ya asociado a otro UID" });
    }

    const now = new Date();
    if (existingByUid) {
      await conn.query(
        "UPDATE cirenio_persons SET cuil = ?, dni = ?, gender = ?, updated_at = ? WHERE uid = ?",
        [cuil, dni, gender, now, uid]
      );
      await conn.commit();
      return res.status(200).json({
        ok: true,
        created: false,
        person: { uid, cuil, dni, gender, updated_at: now.toISOString() }
      });
    }

    await conn.query(
      "INSERT INTO cirenio_persons (uid, cuil, dni, gender, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
      [uid, cuil, dni, gender, now, now]
    );
    await conn.commit();
    return res.status(201).json({
      ok: true,
      created: true,
      person: { uid, cuil, dni, gender, created_at: now.toISOString(), updated_at: now.toISOString() }
    });
  } catch (error) {
    if (conn) {
      await conn.rollback().catch(() => {});
    }
    console.error("[POST /api/core/persons] Error:", error);
    return res.status(500).json({ error: "Ocurrió un error inesperado. Intente nuevamente más tarde." });
  } finally {
    conn?.release();
  }
});

app.get("/api/clients/:cuil", async (req, res) => {
  const identifier = normalizeIdentifier(req.params.cuil);
  let responded = false;

  const sendResponse = (status, data) => {
    if (responded) return;
    responded = true;
    return res.status(status).json(data);
  };

  try {
    if (!isValidIdentifier(identifier)) {
      return sendResponse(400, { error: "CUIL inválido" });
    }

    console.log(`[/api/clients/:cuil] Resolviendo persona para identificador: ${identifier}`);
    const resolved = await resolveCirenioPerson(identifier);
    if (!resolved) {
      return sendResponse(404, { error: "CUIL no encontrado" });
    }

    const data = await fetchCoreApiDataByUid(resolved.person.uid, identifier, resolved.person);
    console.log(`[/api/clients/:cuil] Datos obtenidos exitosamente para identificador: ${identifier}`);
    return sendResponse(200, data);
  } catch (error) {
    if (responded) {
      console.warn(`[/api/clients/:cuil] Intento de respuesta duplicada para identificador: ${identifier}`);
      return;
    }

    console.error(`[/api/clients/:cuil] Error procesando solicitud para identificador: ${identifier}`);
    console.error(`[/api/clients/:cuil] Error name: ${error.name}`);
    console.error(`[/api/clients/:cuil] Error message: ${error.message}`);
    if (error.stack) {
      console.error(`[/api/clients/:cuil] Stack trace:`, error.stack);
    }

    if (error.message === "CUIL no encontrado") {
      return sendResponse(404, { error: error.message });
    }

    if (error.message === "MULTIPLE_PERSON_MATCHES") {
      return sendResponse(409, {
        error: "multiple_person_matches",
        message: "No se pudo determinar una persona única para el DNI consultado."
      });
    }

    return sendResponse(500, { error: "Ocurrió un error inesperado. Intente nuevamente más tarde." });
  }
});

async function handleClienteMetricsRequest(req, res, { source }) {
  const identifier = normalizeIdentifier(req.params.cuil);
  if (!isValidIdentifier(identifier)) {
    return res.status(400).json({ error: "CUIL inválido" });
  }

  const [rows] = await db.query("SELECT * FROM cuil_metrics WHERE cuil = ? ORDER BY job_id DESC LIMIT 1", [identifier]);
  const row = rows[0];

  if (!row) {
    return res.status(404).json({ error: "CUIL no encontrado" });
  }

  // Do not expose internal job_id in the public API response.
  // eslint-disable-next-line no-unused-vars
  const { job_id: _jobId, ...publicRow } = row;

  return res.json({
    ...publicRow,
    updated_at: row.updated_at ? new Date(row.updated_at).toISOString() : null,
    source
  });
}

app.get("/api/health", (req, res) => {
  res.json({ status: "ok" });
});

app.use((req, res) => {
  res.status(404).json({ error: "Ruta no encontrada" });
});

app.listen(PORT, () => {
  console.log(`API escuchando en http://localhost:${PORT}`);
});
