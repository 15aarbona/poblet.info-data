export type SummaryCreatorRow = {
  creador: string;
  platform: string;
  content_count: number;
  visualizaciones: number;
  likes: number;
  /** Màxim de seguidors observat en aquesta xarxa (dades del snapshot). */
  seguidores: number;
};

export type SummaryPayload = {
  generated_at: string;
  run_id: string;
  snapshot_rows: number;
  platforms: Record<
    string,
    {
      content_count: number;
      total_visualizaciones: number;
      total_likes: number;
      total_comentarios: number;
      /** Màxim de seguidors entre totes les files d’aquesta plataforma al snapshot. */
      max_seguidores: number;
    }
  >;
  creators: SummaryCreatorRow[];
};

/** Ordre consistent per a llegendes i comparacions entre gràfics. */
export const PLATFORM_ORDER = ["instagram", "tiktok", "youtube", "twitch"] as const;
export type PlatformId = (typeof PLATFORM_ORDER)[number];

export const PLATFORM_COLORS_RGB: Record<PlatformId, string> = {
  instagram: "225, 48, 108",
  tiktok: "125, 211, 252",
  youtube: "248, 113, 113",
  twitch: "167, 139, 250",
};

export function comparePlatforms(a: string, b: string): number {
  const ia = PLATFORM_ORDER.indexOf(a as PlatformId);
  const ib = PLATFORM_ORDER.indexOf(b as PlatformId);
  return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
}

export function slugify(name: string): string {
  const s = name
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return s || "creador";
}

/** Assigna slugs únics per a getStaticPaths (evita col·lisions). */
export function slugMapForCreators(names: Iterable<string>): Map<string, string> {
  const used = new Map<string, number>();
  const out = new Map<string, string>();
  const sorted = [...new Set(names)].sort((a, b) => a.localeCompare(b, "ca"));
  for (const name of sorted) {
    const base = slugify(name);
    const count = used.get(base) ?? 0;
    const slug = count === 0 ? base : `${base}-${count + 1}`;
    used.set(base, count + 1);
    out.set(name, slug);
  }
  return out;
}

export type SnapshotRow = {
  platform: string;
  content_id: string;
  creador: string;
  nick: string | null;
  seguidores: number | null;
  fecha_publicacion: string | null;
  tipo_publicacion: string | null;
  likes: number | null;
  comentarios: number | null;
  visualizaciones: number | null;
  url: string | null;
  titulo: string | null;
  extracted_at: string;
  run_id: string;
};

export type LatestSnapshotPayload = {
  generated_at: string;
  run_id: string;
  rows: SnapshotRow[];
};

/** Agregats per data de publicació (magatzem parquet), per mes o setmana. */
export type CreatorPublicationSlice = {
  chart_labels: string[];
  bucket_starts_iso: string[];
  platforms: Record<
    string,
      {
        visualizaciones: number[];
        likes: number[];
        comentarios: number[];
        pieces: number[];
        /** Màxim de seguidors entre peces publicades en el bucket (per data de publicació). */
        seguidores: number[];
      }
  >;
};

export type CreatorTimeseriesPayload = {
  source: string;
  dedupe: string;
  default_granularity: string;
  granularities: {
    month: { creators: Record<string, CreatorPublicationSlice> };
    week: { creators: Record<string, CreatorPublicationSlice> };
  };
};

/** Per a barres/donut: només plataformes amb valor > 0 (mètriques no aplicables → s’oculten). */
export function filterPositiveSeries(
  labels: string[],
  values: number[],
  colors?: string[],
): { labels: string[]; values: number[]; colors?: string[] } {
  const li: string[] = [];
  const vi: number[] = [];
  const ci: string[] = [];
  for (let i = 0; i < values.length; i++) {
    if ((values[i] ?? 0) > 0) {
      li.push(labels[i]!);
      vi.push(values[i]!);
      if (colors) ci.push(colors[i]!);
    }
  }
  return colors ? { labels: li, values: vi, colors: ci } : { labels: li, values: vi };
}

function publicationTimestamp(iso: string | null): number {
  if (!iso) return 0;
  const t = Date.parse(iso);
  return Number.isNaN(t) ? 0 : t;
}

/** Top publicacions per visualitzacions (últim snapshot). */
export function topContentForCreator(rows: SnapshotRow[], creador: string, limit: number): SnapshotRow[] {
  const filtered = rows.filter((r) => r.creador === creador);
  filtered.sort((a, b) => {
    const va = a.visualizaciones ?? 0;
    const vb = b.visualizaciones ?? 0;
    if (vb !== va) return vb - va;
    return publicationTimestamp(b.fecha_publicacion) - publicationTimestamp(a.fecha_publicacion);
  });
  return filtered.slice(0, limit);
}

/** Etiqueta curta del tipus de contingut (reels, shorts, etc.). */
export function formatContentKindLabel(row: SnapshotRow): string {
  const t = (row.tipo_publicacion ?? "").trim().toUpperCase();
  const titulo = row.titulo?.trim();
  if (row.platform === "instagram") {
    if (t === "REEL") return "Reel";
    if (t === "PUBLICACION") return "Publicació";
    return t || "Instagram";
  }
  if (row.platform === "youtube") {
    if (t === "SHORT") return "Short";
    if (t === "VIDEO") return "Vídeo";
    return t || "YouTube";
  }
  if (titulo) return titulo;
  return t || "—";
}

/** Cel·la plataforma amb subtipus quan aplica (Instagram · Reel, YouTube · Short…). */
export function formatPlatformWithKind(row: SnapshotRow): string {
  const base = row.platform.charAt(0).toUpperCase() + row.platform.slice(1);
  if (row.platform === "instagram") {
    const k = formatContentKindLabel(row);
    if (k === "Reel" || k === "Publicació") return `Instagram · ${k}`;
  }
  if (row.platform === "youtube") {
    const k = formatContentKindLabel(row);
    if (k === "Short" || k === "Vídeo") return `YouTube · ${k}`;
  }
  return base;
}

/** Text del card «Per plataforma»: omet mètriques inexistents (0) a la xarxa. */
export function formatPlatformCardHint(
  p: SummaryPayload["platforms"][string],
  fmt: Intl.NumberFormat,
): string {
  const parts: string[] = [`${fmt.format(p.content_count)} continguts`];
  const maxSeg = p.max_seguidores ?? 0;
  if (maxSeg > 0) parts.push(`fins a ${fmt.format(Math.round(maxSeg))} seguidors`);
  if (p.total_likes > 0) parts.push(`${fmt.format(Math.round(p.total_likes))} m’agrada`);
  if (p.total_comentarios > 0) parts.push(`${fmt.format(Math.round(p.total_comentarios))} comentaris`);
  return parts.join(" · ");
}

export function aggregateCreators(rows: SummaryCreatorRow[]) {
  const m = new Map<
    string,
    { visualizaciones: number; content_count: number; seguidores: number; platforms: Set<string> }
  >();
  for (const r of rows) {
    let cur = m.get(r.creador);
    if (!cur) {
      cur = { visualizaciones: 0, content_count: 0, seguidores: 0, platforms: new Set<string>() };
      m.set(r.creador, cur);
    }
    cur.visualizaciones += r.visualizaciones;
    cur.content_count += r.content_count;
    cur.seguidores += r.seguidores ?? 0;
    cur.platforms.add(r.platform);
  }
  return [...m.entries()]
    .map(([creador, v]) => ({
      creador,
      visualizaciones: v.visualizaciones,
      content_count: v.content_count,
      seguidores: v.seguidores,
      platforms: v.platforms.size,
    }))
    .sort((a, b) => b.visualizaciones - a.visualizaciones);
}
