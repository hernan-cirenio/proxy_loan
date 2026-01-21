# Loan Proxy

Modulo para importar CSVs, recalcular variables por CUIL y exponer una API REST.

## Componentes
- Node.js API + UI: carga de archivos, login y consulta `/api/clientes/:cuil`.
- Python importer: procesa jobs asincronos y recalcula el agregado.
- SQLite: almacenamiento de jobs y variables agregadas.

## Configuracion
1. Crear `.env` desde `.env.example`.
2. Instalar dependencias Node: `npm install`.
3. Instalar deps Python: `python3 -m venv .venv && . .venv/bin/activate && pip install -r worker/requirements.txt`.

## Ejecucion
- API/UI: `npm start`.
- Worker: `python3 worker/importer.py` (procesa jobs `uploaded`).

## Docker
1. Crear `.env` desde `.env.example`.
2. Levantar servicios: `docker compose up --build`.
3. UI/API: `http://localhost:3000`.

## Seguridad (API Key)
- Los endpoints bajo `/api/*` requieren **sesión** (login UI) **o** **API KEY**.
- Configurar `API_KEY_CIRENIO` y/o `API_KEY_BESMART` en `.env`.
- En Postman enviar:
  - Header `x-api-key: <API_KEY>` o `Authorization: Bearer <API_KEY>`.

## Auditoría (API requests)
- Cuando se autentica con API key, la API registra cada request en la tabla `api_requests` con:
  - `api_client` (cirenio / besmart)
  - `ip` (desde `X-Forwarded-For` / `req.ip`)
  - `path`, `method`, `status_code`, `cuil`, `user_agent`

## Desarrollo local sin DO Spaces
- Setear `LOCAL_STORAGE=true` y `LOCAL_STORAGE_DIR=./data/uploads` en `.env`.
- El API guardara los archivos en disco y el worker los lee desde ese directorio.

## Modo un solo contenedor (sin worker separado)
- Setear `INLINE_PROCESS=true` en `.env`.
- Levantar solo API: `docker compose up --build api`.

## Endpoints
- UI: `GET /` (login requerido).
- API health: `GET /api/health`.
- API cliente: `GET /api/clientes/:cuil` (404 si no existe).
- API core (mock): `GET /api/core/clientes/:cuil` (misma interfaz/payload que `/api/clientes/:cuil`).

## Notas
- Cada importacion elimina los datos previos y recalcula todo.
- Las formulas actuales son heuristicas. Cuando se definan las reglas finales, se ajustan en `worker/importer.py`.
