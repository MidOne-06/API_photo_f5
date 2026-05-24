# Docker

## Antes de levantar

1. Completa tu archivo `.env` usando `.env.example` como referencia.
2. Si ya tienes una sesion de Telegram activa, copia estos archivos a `data/`:
   - `session_bot_ft.session`
   - `session_bot_ft.session-journal`

## Levantar el servicio

```bash
docker compose up --build -d
```

Si es primer despliegue en Linux:

```bash
mkdir -p data
```

Si el contenedor reporta errores de sesion SQLite por permisos (`unable to open database file` o `readonly database`), corrige permisos en `data/` para el usuario `app` del contenedor (`UID=100`, `GID=101`):

```bash
sudo chown -R 100:101 data
sudo chmod -R u+rwX,g+rwX data
```

## Ver estado

```bash
docker compose ps
docker logs -f api_photo_f4
```

El healthcheck consulta `GET /health` dentro del contenedor y solo marca el servicio como sano cuando:

- PostgreSQL responde.
- Telegram esta conectado.
- El servicio no esta en estado de baneo.

## Persistencia

La carpeta `data/` del proyecto se monta en `/app/data` dentro del contenedor y guarda:

- sesion de Telethon
- `api_state.json`

## Notas

- La base de datos PostgreSQL sigue siendo externa; este compose no crea un contenedor de base.
- El contenedor corre con usuario no root, filesystem raiz en solo lectura y `tmpfs` para `/tmp`.
- Puerto por defecto: `8025`.
