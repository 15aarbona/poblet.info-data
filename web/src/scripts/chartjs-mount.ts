import Chart from "chart.js/auto";

export type BarChartConfig = {
  labels: string[];
  values: number[];
  title: string;
  horizontal: boolean;
  color: string;
};

export type DoughnutChartConfig = {
  labels: string[];
  values: number[];
  colors: string[];
};

export type LineMultiConfig = {
  labels: string[];
  datasets: { label: string; data: number[]; color: string }[];
};

function mountBarChart(chartId: string, config: BarChartConfig) {
  const el = document.getElementById(chartId);
  if (!el || !(el instanceof HTMLCanvasElement)) return;
  if (el.dataset.pobletMounted === "1") return;
  el.dataset.pobletMounted = "1";

  const tick = "#a1a1aa";
  const grid = "#27272a";

  new Chart(el, {
    type: "bar",
    data: {
      labels: config.labels,
      datasets: [
        {
          label: config.title || "Valor",
          data: config.values,
          backgroundColor: `rgba(${config.color}, 0.45)`,
          borderColor: `rgba(${config.color}, 0.95)`,
          borderWidth: 1,
          borderRadius: 4,
        },
      ],
    },
    options: {
      indexAxis: config.horizontal ? "y" : "x",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: tick }, grid: { color: grid } },
        y: { ticks: { color: tick }, grid: { color: grid } },
      },
    },
  });
}

function mountDoughnutChart(chartId: string, config: DoughnutChartConfig) {
  const el = document.getElementById(chartId);
  if (!el || !(el instanceof HTMLCanvasElement)) return;
  if (el.dataset.pobletMounted === "1") return;
  el.dataset.pobletMounted = "1";

  const tick = "#a1a1aa";

  new Chart(el, {
    type: "doughnut",
    data: {
      labels: config.labels,
      datasets: [
        {
          data: config.values,
          backgroundColor: config.colors.map((c) => `rgba(${c}, 0.55)`),
          borderColor: config.colors.map((c) => `rgba(${c}, 0.9)`),
          borderWidth: 1,
          hoverOffset: 6,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      cutout: "58%",
      plugins: {
        tooltip: {
          callbacks: {
            label(ctx) {
              const values = ctx.dataset.data as number[];
              const total = values.reduce((a, b) => a + b, 0);
              const v = typeof ctx.parsed === "number" ? ctx.parsed : 0;
              const pct = total > 0 ? ((v / total) * 100).toFixed(1) : "0";
              const n = new Intl.NumberFormat("ca-ES").format(v);
              const label = ctx.label ?? "";
              return `${label}: ${n} (${pct}%)`;
            },
          },
        },
        legend: {
          position: "bottom",
          labels: { color: tick, boxWidth: 12, padding: 12 },
        },
      },
    },
  });
}

function mountLineMulti(chartId: string, config: LineMultiConfig) {
  const el = document.getElementById(chartId);
  if (!el || !(el instanceof HTMLCanvasElement)) return;
  if (el.dataset.pobletMounted === "1") return;
  el.dataset.pobletMounted = "1";

  const tick = "#a1a1aa";
  const grid = "#27272a";

  const ds = config.datasets.map((d) => ({
    label: d.label,
    data: d.data,
    borderColor: `rgba(${d.color}, 0.95)`,
    backgroundColor: `rgba(${d.color}, 0.12)`,
    tension: 0.25,
    fill: false,
    borderWidth: 2,
    pointRadius: 3,
    pointHoverRadius: 5,
  }));

  new Chart(el, {
    type: "line",
    data: { labels: config.labels, datasets: ds },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: true, labels: { color: tick, boxWidth: 12 } },
        title: { display: false },
      },
      scales: {
        x: {
          ticks: { color: tick, maxRotation: 45, minRotation: 0 },
          grid: { color: grid },
        },
        y: { ticks: { color: tick }, grid: { color: grid } },
      },
    },
  });
}

function decodePayload<T>(encoded: string | null | undefined): T | null {
  if (!encoded) return null;
  try {
    const json = decodeURIComponent(encoded);
    return JSON.parse(json) as T;
  } catch {
    return null;
  }
}

export function mountAllChartsFromDom() {
  document.querySelectorAll<HTMLElement>('[data-poblet-chart="bar"]').forEach((host) => {
    const chartId = host.dataset.chartId;
    const payload = decodePayload<BarChartConfig>(host.dataset.pobletCfg);
    if (chartId && payload) mountBarChart(chartId, payload);
  });

  document.querySelectorAll<HTMLElement>('[data-poblet-chart="doughnut"]').forEach((host) => {
    const chartId = host.dataset.chartId;
    const payload = decodePayload<DoughnutChartConfig>(host.dataset.pobletCfg);
    if (chartId && payload) mountDoughnutChart(chartId, payload);
  });

  document.querySelectorAll<HTMLElement>('[data-poblet-chart="line"]').forEach((host) => {
    const chartId = host.dataset.chartId;
    const payload = decodePayload<LineMultiConfig>(host.dataset.pobletCfg);
    if (chartId && payload) mountLineMulti(chartId, payload);
  });
}

function scheduleMount() {
  mountAllChartsFromDom();
  void import("./creator-explorer").then((m) => m.mountAllCreatorExplorers());
}

if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scheduleMount);
  } else {
    scheduleMount();
  }
}
