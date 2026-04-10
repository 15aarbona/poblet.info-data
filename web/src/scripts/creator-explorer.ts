import Chart from "chart.js/auto";
import type { Chart as ChartJS, ChartConfiguration } from "chart.js/auto";

type Series = {
  visualizaciones: number[];
  likes: number[];
  comentarios: number[];
  pieces: number[];
  seguidores: number[];
};

type GranularityBlock = {
  chartLabels: string[];
  bucketStartsIso: string[];
  platforms: Record<string, Series>;
};

export type CreatorExplorerCfg = {
  granularities: Record<string, GranularityBlock>;
  defaultGranularity: string;
  platformColors: Record<string, string>;
};

const METRIC_META = [
  { key: "visualizaciones", label: "Visualitzacions" },
  { key: "likes", label: "M’agrada" },
  { key: "comentarios", label: "Comentaris" },
  { key: "pieces", label: "Peces publicades" },
  { key: "seguidores", label: "Seguidors (màx. al període)" },
] as const;

const GRAN_LABELS: Record<string, string> = {
  month: "Mes (per data de publicació)",
  week: "Setmana (dilluns, per data de publicació)",
};

const PRESETS: { id: string; label: string; days: number | null }[] = [
  { id: "all", label: "Tot l’històric", days: null },
  { id: "1y", label: "Últim any (~365 dies)", days: 365 },
  { id: "6m", label: "Últims 6 mesos (~183 dies)", days: 183 },
  { id: "3m", label: "Últims 3 mesos (~92 dies)", days: 92 },
  { id: "1m", label: "Últim mes (~31 dies)", days: 31 },
];

function decode(cfgEnc: string | undefined): CreatorExplorerCfg | null {
  if (!cfgEnc) return null;
  try {
    return JSON.parse(decodeURIComponent(cfgEnc)) as CreatorExplorerCfg;
  } catch {
    return null;
  }
}

function titleCasePlat(p: string) {
  return p.charAt(0).toUpperCase() + p.slice(1);
}

function borderForDataset(index: number, baseRgb: string): string {
  const parts = baseRgb.split(",").map((x) => parseInt(x.trim(), 10));
  const r = Number.isFinite(parts[0]) ? parts[0]! : 148;
  const g = Number.isFinite(parts[1]) ? parts[1]! : 163;
  const b = Number.isFinite(parts[2]) ? parts[2]! : 184;
  const shift = index * 0.12;
  const r2 = Math.min(255, Math.round(r + 40 * shift));
  const g2 = Math.min(255, Math.round(g + 25 * (1 - shift)));
  const b2 = Math.min(255, Math.round(b + 35 * shift));
  return `rgba(${r2}, ${g2}, ${b2}, 0.95)`;
}

function platformKeys(cfg: CreatorExplorerCfg): string[] {
  const s = new Set<string>();
  for (const b of Object.values(cfg.granularities)) {
    Object.keys(b.platforms ?? {}).forEach((k) => s.add(k));
  }
  return [...s].sort();
}

function availableGranularities(cfg: CreatorExplorerCfg): string[] {
  return Object.entries(cfg.granularities)
    .filter(([, block]) => (block.chartLabels?.length ?? 0) > 0)
    .map(([k]) => k);
}

/** Primer índex de bucket amb inici >= tall temporal (o 0). Sempre fins al darrer bucket (fins a l’actualitat). */
function startIndexForPreset(bucketStartsIso: string[], presetId: string): number {
  const preset = PRESETS.find((p) => p.id === presetId);
  const days = preset?.days;
  if (days == null || bucketStartsIso.length === 0) return 0;
  const cutoff = Date.now() - days * 86400000;
  for (let i = 0; i < bucketStartsIso.length; i++) {
    const t = new Date(bucketStartsIso[i]!).getTime();
    if (!Number.isNaN(t) && t >= cutoff) return i;
  }
  return 0;
}

function mountOne(host: HTMLElement) {
  if (host.dataset.pobletExplorerReady === "1") return;

  const cfg = decode(host.dataset.pobletCfg);
  const uid = host.dataset.explorerUid ?? "ex";
  if (!cfg) return;

  const granKeys = availableGranularities(cfg);
  if (granKeys.length === 0) return;

  let currentGran = granKeys.includes(cfg.defaultGranularity)
    ? cfg.defaultGranularity
    : granKeys[0]!;
  let currentPreset = "all";

  const canvas = host.querySelector("canvas");
  if (!(canvas instanceof HTMLCanvasElement)) return;

  const controls = document.createElement("div");
  controls.className =
    "mb-4 flex flex-col gap-4 rounded-xl border border-zinc-800 bg-zinc-900/40 p-4 text-sm text-zinc-300";

  const granWrap = document.createElement("div");
  granWrap.className = "flex flex-col gap-1";
  const granLab = document.createElement("label");
  granLab.className = "text-xs text-zinc-500";
  granLab.textContent = "Agrupació temporal";
  const granSel = document.createElement("select");
  granSel.className =
    "max-w-md rounded-lg border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-zinc-200 focus:border-indigo-500 focus:outline-none";
  for (const k of granKeys) {
    const o = document.createElement("option");
    o.value = k;
    o.textContent = GRAN_LABELS[k] ?? k;
    granSel.append(o);
  }
  granSel.value = currentGran;
  granWrap.append(granLab, granSel);

  const presetWrap = document.createElement("div");
  presetWrap.className = "flex flex-col gap-1";
  const presetLab = document.createElement("label");
  presetLab.className = "text-xs text-zinc-500";
  presetLab.textContent = "Període (fins a l’actualitat, buckets sense dades = 0)";
  const presetSel = document.createElement("select");
  presetSel.className =
    "max-w-md rounded-lg border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-zinc-200 focus:border-indigo-500 focus:outline-none";
  for (const p of PRESETS) {
    const o = document.createElement("option");
    o.value = p.id;
    o.textContent = p.label;
    presetSel.append(o);
  }
  presetSel.value = currentPreset;
  presetWrap.append(presetLab, presetSel);

  const platNames = platformKeys(cfg);
  const platField = document.createElement("fieldset");
  platField.className = "min-w-0";
  platField.innerHTML =
    '<legend class="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">Xarxes socials</legend>';
  const platWrap = document.createElement("div");
  platWrap.className = "flex flex-wrap gap-x-4 gap-y-2";
  platNames.forEach((p) => {
    const lab = document.createElement("label");
    lab.className = "flex cursor-pointer items-center gap-2";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = p;
    cb.checked = true;
    cb.id = `${uid}-plat-${p}`;
    cb.className = "rounded border-zinc-600 bg-zinc-900";
    lab.append(cb);
    const span = document.createElement("span");
    span.textContent = titleCasePlat(p);
    lab.append(span);
    platWrap.append(lab);
  });
  platField.append(platWrap);

  const metField = document.createElement("fieldset");
  metField.className = "min-w-0";
  metField.innerHTML =
    '<legend class="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">Mètriques (es poden superposar)</legend>';
  const metWrap = document.createElement("div");
  metWrap.className = "flex flex-wrap gap-x-4 gap-y-2";
  METRIC_META.forEach((m) => {
    const lab = document.createElement("label");
    lab.className = "flex cursor-pointer items-center gap-2";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = m.key;
    cb.checked = true;
    cb.id = `${uid}-met-${m.key}`;
    cb.className = "rounded border-zinc-600 bg-zinc-900";
    lab.append(cb);
    const span = document.createElement("span");
    span.textContent = m.label;
    lab.append(span);
    metWrap.append(lab);
  });
  metField.append(metWrap);

  const normLab = document.createElement("label");
  normLab.className = "mt-2 flex cursor-pointer items-center gap-2";
  const normCb = document.createElement("input");
  normCb.type = "checkbox";
  normCb.className = "rounded border-zinc-600 bg-zinc-900";
  normLab.append(normCb);
  const normSpan = document.createElement("span");
  normSpan.textContent = "Escala normalitzada (0–100% del màxim visible per sèrie)";
  normLab.append(normSpan);

  controls.append(granWrap, presetWrap, platField, metField, normLab);
  host.insertBefore(controls, host.firstElementChild);

  let chartInstance: ChartJS | null = null;

  const tick = "#a1a1aa";
  const grid = "#27272a";

  function getBlock(): GranularityBlock | undefined {
    return cfg.granularities[currentGran];
  }

  const rebuild = () => {
    const block = getBlock();
    if (!block?.chartLabels?.length) {
      if (chartInstance) {
        chartInstance.destroy();
        chartInstance = null;
      }
      return;
    }

    const i0 = startIndexForPreset(block.bucketStartsIso, currentPreset);
    const i1 = block.chartLabels.length - 1;
    const labels = block.chartLabels.slice(i0, i1 + 1);

    const selectedPlats = [...platWrap.querySelectorAll("input:checked")].map(
      (el) => (el as HTMLInputElement).value,
    );
    const selectedMetrics = [...metWrap.querySelectorAll("input:checked")].map(
      (el) => (el as HTMLInputElement).value,
    );

    const datasets: ChartConfiguration<"line">["data"]["datasets"] = [];
    let di = 0;
    for (const p of selectedPlats) {
      const ser = block.platforms[p];
      if (!ser) continue;
      const baseRgb = cfg.platformColors[p] ?? "148, 163, 184";
      for (const mk of selectedMetrics) {
        const meta = METRIC_META.find((m) => m.key === mk);
        if (!meta) continue;
        const arr = ser[meta.key as keyof Series];
        if (!arr) continue;
        const raw = [...arr].slice(i0, i1 + 1) as number[];
        let data = raw;
        if (normCb.checked) {
          const mx = Math.max(...data.map((v) => Math.abs(v)), 1e-9);
          data = data.map((v) => (v / mx) * 100);
        }
        datasets.push({
          label: `${titleCasePlat(p)} · ${meta.label}${normCb.checked ? " (%)" : ""}`,
          data,
          borderColor: borderForDataset(di, baseRgb),
          backgroundColor: "transparent",
          tension: 0.25,
          fill: false,
          borderWidth: 2,
          pointRadius: 3,
          pointHoverRadius: 5,
        });
        di++;
      }
    }

    if (chartInstance) {
      chartInstance.destroy();
      chartInstance = null;
    }
    if (datasets.length === 0 || labels.length === 0) return;

    chartInstance = new Chart(canvas, {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { color: tick, boxWidth: 12 } },
        },
        scales: {
          x: {
            ticks: { color: tick, maxRotation: 45 },
            grid: { color: grid },
          },
          y: {
            ticks: { color: tick },
            grid: { color: grid },
            title: {
              display: normCb.checked,
              text: "% respecte al màxim de cada sèrie al període",
              color: "#71717a",
            },
          },
        },
      },
    });
  };

  granSel.addEventListener("change", () => {
    currentGran = granSel.value;
    rebuild();
  });

  presetSel.addEventListener("change", () => {
    currentPreset = presetSel.value;
    rebuild();
  });

  platWrap.addEventListener("change", rebuild);
  metWrap.addEventListener("change", rebuild);
  normCb.addEventListener("change", rebuild);

  rebuild();
  host.dataset.pobletExplorerReady = "1";
}

export function mountAllCreatorExplorers() {
  document.querySelectorAll<HTMLElement>("[data-poblet-creator-explorer]").forEach(mountOne);
}
