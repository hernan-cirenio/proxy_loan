import express from "express";
import session from "express-session";
import multer from "multer";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getDb } from "./db.js";

const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || "change-me";
const APP_USER = process.env.APP_USER || "admin";
const APP_PASS = process.env.APP_PASS || "secret";

const DO_SPACES_ENDPOINT = process.env.DO_SPACES_ENDPOINT;
const DO_SPACES_BUCKET = process.env.DO_SPACES_BUCKET;
const DO_SPACES_KEY = process.env.DO_SPACES_KEY;
const DO_SPACES_SECRET = process.env.DO_SPACES_SECRET;
const DO_SPACES_REGION = process.env.DO_SPACES_REGION || "us-east-1";
const LOCAL_STORAGE = process.env.LOCAL_STORAGE === "true";
const LOCAL_STORAGE_DIR = process.env.LOCAL_STORAGE_DIR || "data/uploads";
const INLINE_PROCESS = process.env.INLINE_PROCESS === "true";
const PYTHON_BIN = process.env.PYTHON_BIN || "python3";

const app = express();
const db = getDb();

const upload = multer({ dest: LOCAL_STORAGE_DIR });

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
        <button class="ghost" type="submit">Salir</button>
      </form>
    </section>

    ${message ? `<div class="alert success">${message}</div>` : ""}

    <section class="card">
      <h2>Nuevo procesamiento</h2>
      <form class="form grid" action="/upload" method="post" enctype="multipart/form-data">
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
    </section>

    <section class="card">
      <h2>Buscar datos por CUIL</h2>
      <p class="muted">Buscá un CUIL y revisá los valores calculados.</p>
      <form class="form grid" id="cuil-qa-form">
        <label>
          CUIL
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
  const jobs = db
    .prepare("SELECT * FROM jobs ORDER BY id DESC LIMIT 5")
    .all();
  const message = req.query.message === "ok" ? "Archivos recibidos. El job queda en cola." : null;
  res.send(renderDashboard({ jobs, message }));
});

app.post(
  "/upload",
  requireAuth,
  upload.fields([
    { name: "detalle", maxCount: 1 },
    { name: "convenios", maxCount: 1 }
  ]),
  async (req, res) => {
    const detalleFile = req.files?.detalle?.[0];
    const conveniosFile = req.files?.convenios?.[0];

    if (!detalleFile || !conveniosFile) {
      const jobs = db
        .prepare("SELECT * FROM jobs ORDER BY id DESC LIMIT 5")
        .all();
      return res.status(400).send(renderDashboard({ jobs, message: "Faltan archivos" }));
    }

    const now = new Date().toISOString();
    const insertJob = db.prepare(
      "INSERT INTO jobs (created_at, status, detalle_name, convenios_name) VALUES (?, ?, ?, ?)"
    );
    const result = insertJob.run(now, "pending_upload", detalleFile.originalname, conveniosFile.originalname);
    const jobId = result.lastInsertRowid;

    try {
      if (LOCAL_STORAGE) {
        const detalleKey = path.join("imports", String(jobId), "detalle.csv");
        const conveniosKey = path.join("imports", String(jobId), "convenios.csv");
        const detalleDest = path.join(LOCAL_STORAGE_DIR, detalleKey);
        const conveniosDest = path.join(LOCAL_STORAGE_DIR, conveniosKey);

        fs.mkdirSync(path.dirname(detalleDest), { recursive: true });
        fs.mkdirSync(path.dirname(conveniosDest), { recursive: true });

        fs.renameSync(detalleFile.path, detalleDest);
        fs.renameSync(conveniosFile.path, conveniosDest);

        db.prepare("UPDATE jobs SET status = ?, detalle_key = ?, convenios_key = ? WHERE id = ?").run(
          "uploaded",
          detalleKey,
          conveniosKey,
          jobId
        );
      } else {
        const s3 = createS3Client();
        const detalleKey = `imports/${jobId}/detalle.csv`;
        const conveniosKey = `imports/${jobId}/convenios.csv`;

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

        db.prepare("UPDATE jobs SET status = ?, detalle_key = ?, convenios_key = ? WHERE id = ?").run(
          "uploaded",
          detalleKey,
          conveniosKey,
          jobId
        );
      }

      if (INLINE_PROCESS) {
        runInlineWorker();
      }

      res.redirect("/?message=ok");
    } catch (error) {
      db.prepare("UPDATE jobs SET status = ?, message = ? WHERE id = ?").run("failed", error.message, jobId);
      const jobs = db
        .prepare("SELECT * FROM jobs ORDER BY id DESC LIMIT 5")
        .all();
      res.status(500).send(renderDashboard({ jobs, message: "Error subiendo archivos" }));
    } finally {
      if (!LOCAL_STORAGE) {
        fs.unlink(detalleFile.path, () => {});
        fs.unlink(conveniosFile.path, () => {});
      }
    }
  }
);

app.get("/api/clientes/:cuil", (req, res) => {
  const cuil = String(req.params.cuil || "").trim();
  const row = db
    .prepare("SELECT * FROM cuil_metrics WHERE cuil = ?")
    .get(cuil);

  if (!row) {
    return res.status(404).json({ error: "CUIL no encontrado" });
  }

  return res.json(row);
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok" });
});

app.use((req, res) => {
  res.status(404).json({ error: "Ruta no encontrada" });
});

app.listen(PORT, () => {
  console.log(`API escuchando en http://localhost:${PORT}`);
});
