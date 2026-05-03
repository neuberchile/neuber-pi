# neuber-pi

**Generador de Proforma Invoice (Word .docx)** para Neuber. Servicio dedicado: una request, una PI generada con número correlativo y formato establecido.

## Versión actual

**v2.11** (regenerate_pi_with_signature endpoint para Bucket 1) — sesión 3.32 (2026-05-03)

## Health check

```
GET https://neuber-pi-production.up.railway.app/health
```

Retorna `{service, status, version}`.

## Endpoints

| Endpoint | Método | Auth | Función |
|---|---|---|---|
| `/webhook` | POST | (Pipedrive) | Genera PI cuando deal pasa a stage 6 (Cerrado) |
| `/generate_pi/<deal_id>` | GET | ⚠️ ninguna | Genera PI manualmente |
| `/regenerate_pi_with_signature/<deal_id>` | POST | ⚠️ ninguna | Regenera PI con firma proveedor inyectada (Bucket 1) |
| `/bank_hash/register` | GET/POST | ⚠️ ninguna | Registra hash de datos bancarios |
| `/health` | GET | — | Status |

## Flow operativo

### Generación normal (al cierre del deal)

1. Pipedrive webhook deal stage 1→6
2. neuber-pi obtiene `deal_data` via API
3. Calcula próximo PI number (lee nota pinned en deal 467)
4. Genera .docx con datos del deal + items + datos bancarios proveedor
5. Adjunta al deal Ventas
6. Bumpea contador en deal 467

### Regenerate con firma (Bucket 1)

1. Luke llama POST `/regenerate_pi_with_signature/<deal_id>` con:
   - `pi_number` (reusa, NO incrementa)
   - `signature_b64` (imagen firma proveedor)
   - `signature_mime` ('image/png' default)
2. neuber-pi regenera el .docx desde deal_data + inserta imagen en celda firma proveedor
3. Adjunta al deal con nombre `PI_NNNN_..._firmada.docx`
4. Retorna `{filename, content_b64}` para que Luke la propague a Drive y deal Op

## Env vars requeridas (Railway)

```
PIPEDRIVE_API           — Token API Pipedrive (con fallback hardcoded en código, debt)
```

Sin Anthropic, sin SendGrid (no envía emails).
Sin Google OAuth (no toca Drive directamente).

## Deploy

Auto-deploy a Railway en push a `main`.

## Dependencies

```
flask==3.0.0
requests==2.31.0
python-docx==1.1.0      ← key library para generar el .docx
gunicorn==21.2.0
```

## Arquitectura

neuber-pi es **stateless** (no mantiene estado en memoria). Todo el estado vive en Pipedrive (deal 467 master note para counter, hashes bank_data).

## Issues conocidos

- **Sin auth en `/generate_pi` y `/regenerate_pi_with_signature`** — cualquiera puede quemar números de PI o adjuntar archivos arbitrarios. Pendiente sesión 3.33+: agregar `PI_ADMIN_TOKEN` env var coordinada con Luke.
- **TOKEN con fallback hardcoded** en línea 26 (mejor que Luke/Leia que son plain hardcoded, pero igual pendiente migrar a env-only).

## Documentos relacionados

Ver Project Knowledge `Neuber`.
