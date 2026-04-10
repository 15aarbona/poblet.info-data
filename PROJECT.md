# Proyecto Poblet — Documentación completa

Este repositorio recoge un **pipeline local** que normaliza datos de redes sociales (Instagram, TikTok, YouTube, Twitch), los acumula en un **almacén Parquet** y genera **JSON estáticos** consumidos por un **dashboard web** (Astro + Chart.js). No hay backend en tiempo real: todo se actualiza ejecutando el pipeline.

---

## Visión general del flujo

1. **Extracción** (fuera del alcance detallado de `main.py`): scripts en `pipeline/scripts/` y extractores en `pipeline/extractors/*Extractor.py` suelen volcar históricos en Parquet bajo `data/` (p. ej. ficheros `historico_*`).
2. **Limpieza (`DataCleaner`)**: lee esos Parquet, normaliza tipos y escribe un snapshot por plataforma en `data/staging/latest_<plataforma>.parquet`.
3. **Transformación (`DataTransformer`)**: lee el staging, convierte al esquema canónico, **añade filas al warehouse** `data/warehouse/content_observations.parquet` (cada ejecución es un `run_id` con `extracted_at`), y genera los JSON del dashboard en `web/src/data/dashboard/`.
4. **Web (`web/`)**: Astro construye páginas estáticas que importan esos JSON y dibujan gráficos con Chart.js en el cliente (`chartjs-mount.ts` + `creator-explorer.ts`).

```
Extracción → Parquet crudo → DataCleaner → staging/latest_*.parquet
                                              ↓
                                    DataTransformer → warehouse + JSON dashboard
                                              ↓
                                    npm run build (web) → sitio estático
```

---

## Cómo usar el proyecto

### Requisitos

- Python 3 con el entorno del proyecto (p. ej. `.venv` en la raíz).
- Node.js y npm para el frontend.

### Actualizar datos y el dashboard

Desde la raíz del repositorio:

```bash
.venv/bin/python main.py
```

Esto ejecuta en cadena `DataCleaner` y `DataTransformer`. Tras cambiar datos, conviene **reconstruir o levantar la web**:

```bash
npm run dev    # desarrollo con hot reload (sirve desde web/)
# o
npm run build  # genera la salida estática en web/dist
npm run preview
```

Los JSON que lee Astro están en `web/src/data/dashboard/`; si no ejecutas `main.py`, la web seguirá mostrando la última copia generada.

### Métricas de seguidores (interpretación)

- En **snapshot / resumen**: para cada plataforma y cada par (creador, plataforma) se usa el **máximo** de la columna `seguidores` entre las filas del snapshot (refleja seguidores del creador en esa red según los datos capturados).
- **Global “suma de máximos por red”**: se suman los máximos por plataforma del mismo snapshot (no es población única de usuarios; es una magnitud agregada para comparar escala entre redes en el panel).
- **Agregado por creador** (`aggregateCreators` en `web/src/lib/creators.ts`): suma, para cada creador, los máximos por cada red en la que aparece.
- **Evolución temporal (`history.json`)**: por cada `run`, mismos máximos por plataforma; `series_total_seguidores` es la suma de esos máximos por run.
- **Explorer por fecha de publicación** (`creator_timeseries.json`): en cada bucket (mes/semana) la serie `seguidores` es el **máximo** entre piezas publicadas en ese bucket.

---

## Estructura de directorios y archivos

### Raíz

| Ruta | Función |
|------|---------|
| `main.py` | Punto de entrada: `DataCleaner().process()` y `DataTransformer().process()`. |
| `package.json` | Scripts npm que delegan en `web/` (`dev`, `build`, `preview`). |
| `tokens.json` | Credenciales/tokens para extractores (no documentar valores; mantener fuera del control de versiones si aplica). |
| `PROJECT.md` | Este documento. |

### `pipeline/`

| Ruta | Función |
|------|---------|
| `paths.py` | `repo_root()` — raíz del repo para rutas estables. |
| `PipelineGUI.py` | Interfaz gráfica opcional del pipeline (si la usas en tu flujo). |
| `extractors/DataCleaner.py` | Lee Parquet históricos por plataforma, limpia, deduplica por id de contenido y escribe `data/staging/latest_*.parquet`. |
| `extractors/DataTransformer.py` | Mapea staging al esquema canónico, concatena al warehouse, calcula agregados y escribe `summary.json`, `latest_snapshot.json`, `history.json`, `creator_timeseries.json`. Incluye extensión de buckets temporales hasta la fecha actual y relleno con ceros donde no hay datos. |
| `extractors/Extractor.py` | Base o utilidades comunes de extractores. |
| `extractors/InstagramExtractor.py`, `TikTokExtractor.py`, `YoutubeExtractor.py`, `TwitchExtractor.py` | Lógica de extracción por red. |
| `extractors/__init__.py` | Paquete Python. |
| `scripts/*.py` | Scripts de extracción por red (insta, tiktok, yt, twitch, podcast, etc.). |

### `data/`

| Ruta | Función |
|------|---------|
| `staging/latest_*.parquet` | Snapshot limpio **por plataforma** tras `DataCleaner`. |
| `warehouse/content_observations.parquet` | **Magatzem append-only** de observaciones: cada fila es un contenido en un momento de extracción (`run_id`, `extracted_at`). |

### `web/` — Dashboard Astro

| Ruta | Función |
|------|---------|
| `package.json` | Dependencias: Astro 5, Tailwind 4, Chart.js. Scripts `dev`, `build`, `preview`. |
| `src/layouts/Layout.astro` | HTML base, navegación (Resum, Evolució, Creadors, enlaces a plataformas), footer e import de `chartjs-mount.ts`. |
| `src/styles/global.css` | Estilos globales (Tailwind). |
| `src/lib/creators.ts` | Tipos TypeScript alineados con los JSON, `PLATFORM_ORDER`, colores, `aggregateCreators`, `filterPositiveSeries`, helpers de slug y formato. |
| `src/data/dashboard/*.json` | **Salida del pipeline** empaquetada en el build (no editar a mano salvo pruebas). |
| `src/pages/index.astro` | Resumen global: tarjetas, gráficos por plataforma (vistas, contenidos, seguidores, donut), tabla top creadores. |
| `src/pages/evolucio.astro` | Series por run: totales, seguidores globales y por plataforma, contenidos, likes; tabla de runs. |
| `src/pages/creadors/index.astro` | Lista de creadores agregados, barras (vistas y seguidores), tabla. |
| `src/pages/creadors/[slug].astro` | Ficha del creador: barras por red, `CreatorTrendsExplorer`, detalle tabla, top publicaciones. |
| `src/pages/plataformes/[platform].astro` | Vista por red (`getStaticPaths`: instagram, tiktok, youtube, twitch). |
| `src/components/MetricCard.astro` | Tarjeta métrica con enlace opcional. |
| `src/components/BarChart.astro`, `DoughnutChart.astro`, `LineChartMulti.astro` | Envuelven canvas + payload JSON en `data-*` para Chart.js. |
| `src/components/CreatorTrendsExplorer.astro` | Inyecta configuración codificada en URL para el explorador interactivo (mes/semana, métricas, presets de tiempo). |
| `src/scripts/chartjs-mount.ts` | En `DOMContentLoaded`: monta barras, donut, líneas múltiples y delega en `mountAllCreatorExplorers()`. |
| `src/scripts/creator-explorer.ts` | Lógica del gráfico interactivo por creador: granularidad, **presets** (todo el histórico, último año, 6/3/1 meses), checkboxes de redes y métricas (incl. **Seguidores**), normalización 0–100%. |

---

## JSON del dashboard (contrato de datos)

Generados por `DataTransformer` en `web/src/data/dashboard/`:

- **`summary.json`**: metadatos (`generated_at`, `run_id`, `snapshot_rows`), `platforms` con totales y `max_seguidores`, `creators[]` por (creador, plataforma) con agregados de contenido y `seguidores` (máx. en ese grupo).
- **`latest_snapshot.json`**: filas detalladas del último run para tablas (publicaciones, enlaces, etc.).
- **`history.json`**: lista de runs con agregados por plataforma; series alineadas `chart_labels`; `series_seguidores`, `series_total_visualizaciones`, `series_total_seguidores`, etc.
- **`creator_timeseries.json`**: por creador, granularidades `month` y `week` con `chart_labels`, `bucket_starts_iso` y por plataforma arrays de `visualizaciones`, `likes`, `comentarios`, `pieces`, `seguidores` (mismo largo que labels; ceros si no hay actividad en el bucket).

Si cambias el esquema en Python, actualiza los tipos en `creators.ts` y las páginas Astro que lean esos campos.

---

## Explorador “Evolución por data de publicació” (UI)

Montado solo en páginas de creador (`CreatorTrendsExplorer.astro` + `creator-explorer.ts`):

- **Agrupación**: mes natural o semana (lunes) según `fecha_publicacion`.
- **Período**: desplegable con *Todo el histórico*, *Último año*, *Últimos 6/3 meses*, *Último mes* (recorta por índice de bucket según antigüedad; el eje llega hasta el último bucket generado en datos, rellenado con 0 en el pipeline).
- **Métricas**: visualizaciones, likes, comentarios, piezas publicadas, **seguidores** (máx. en el bucket).
- **Normalización**: opción para escalar cada serie al 0–100 % de su máximo en el rango visible.

---

## Comandos rápidos

| Objetivo | Comando |
|----------|---------|
| Regenerar staging + warehouse + JSON | `.venv/bin/python main.py` |
| Desarrollo web | `npm run dev` |
| Build producción | `npm run build` |
| Previsualizar build | `npm run preview` |

---

## Notas de mantenimiento

- Tras añadir plataformas o campos, actualizar `_to_canonical` y los agregados en `DataTransformer.py`, luego tipos y UI en `web/`.
- Los gráficos de barra en el resumen **ocultan** plataformas con valor 0 en esa métrica (`filterPositiveSeries`); las series temporales del explorer muestran ceros explícitos en los buckets sin datos.
- El JSON debe ser **estrictamente válido** (p. ej. sin `NaN`): en `_series_for_platform_bucket` se fuerza `.fillna(0)` tras el `reindex` para que `creator_timeseries.json` pueda importarse en Vite/Node.
- Para que la página **Evolució** muestre tendencias, hace falta **más de un run** en el warehouse (varias ejecuciones de `main.py` en fechas distintas).
