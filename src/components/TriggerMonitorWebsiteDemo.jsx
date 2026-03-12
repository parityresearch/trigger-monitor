import { useEffect, useMemo, useState } from "react";
import { motion } from "framer-motion";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  BarChart,
  Bar,
  Legend,
  ReferenceLine,
} from "recharts";
import {
  Search,
  Bell,
  TrendingDown,
  Gauge,
  ChevronRight,
  Filter,
  Download,
  CheckCircle2,
  AlertTriangle,
  Sparkles,
  Layers,
  LineChart as LineChartIcon,
} from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "./ui/card.jsx";
import { Button } from "./ui/button.jsx";
import { Badge } from "./ui/badge.jsx";
import { Input } from "./ui/input.jsx";
import { Separator } from "./ui/separator.jsx";
import { Progress } from "./ui/progress.jsx";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table.jsx";

/**
 * Trigger Monitor UI (Website Demo)
 * - Vite + React + Tailwind
 * - No backend required (mock data)
 * - Swap mock with API responses later
 */

const fmtPct = (x) => (Number.isFinite(x) ? `${(x * 100).toFixed(1)}%` : "—");
const fmtNum = (x, d = 2) => (Number.isFinite(x) ? x.toFixed(d) : "—");

const DATA_SOURCES = [
  "/data/trigger_monitor_demo.json",
  "./data/trigger_monitor_demo.json",
  "/out/trigger_monitor_demo.json",
  "./out/trigger_monitor_demo.json",
];

const EXPLORER_SOURCES = [
  "/data/trigger_explorer.json",
  "./data/trigger_explorer.json",
];

const EMPTY_DEMO = {
  asOf: null,
  portfolio: { flagged: 0, deals: 0, red: 0, yellow: 0 },
  alerts: [],
  deals: [],
};

const ASSET_BASE = (import.meta.env.BASE_URL || "/").replace(/\/?$/, "/");

const BRAND = {
  firm: "Parity Research",
  product: "Trigger Monitor",
  logoSrc: `${ASSET_BASE}branding/LOGO.png`,
  logoFallbackSrc: `${ASSET_BASE}branding/parity-research-logo.svg`,
};

async function loadExplorerData() {
  for (const src of EXPLORER_SOURCES) {
    try {
      const res = await fetch(`${src}?t=${Date.now()}`, { cache: "no-store" });
      if (!res.ok) continue;
      const data = await res.json();
      if (data?.deals) return data;
    } catch { /* try next */ }
  }
  return null;
}

async function loadDemoData() {
  for (const base of DATA_SOURCES) {
    const url = `${base}${base.includes("?") ? "" : `?t=${Date.now()}`}`;
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) continue;
      const data = await res.json();
      if (data && Array.isArray(data.deals)) {
        return data;
      }
    } catch {
      // Try next source.
    }
  }
  return null;
}



function percentRankLabel(p) {
  if (p >= 0.85) return "Severe";
  if (p >= 0.70) return "Moderate";
  return "Normal";
}

function formatMetricValue(metric, value, threshold) {
  const label = String(metric || "").toLowerCase();
  const hasPct = label.includes("%") || label.includes("percent");
  const nums = [value, threshold].filter((v) => Number.isFinite(v));
  const looksPct = nums.length ? nums.every((v) => Math.abs(v) <= 1) : false;
  return hasPct || looksPct ? fmtPct(value) : fmtNum(value);
}

function thresholdSourceLabel(source) {
  const key = String(source || "").toLowerCase();
  if (key === "reported") return "Reported in filing";
  if (key === "override") return "Config override";
  if (key === "force_override") return "Forced override";
  if (key === "schedule") return "Schedule fallback";
  if (key === "schedule_force") return "Schedule override";
  if (key === "missing") return "Missing";
  return "Unknown";
}


function Pill({ tone = "outline", children, className = "" }) {
  return (
    <Badge variant={tone} className={`rounded-full px-2.5 py-1 text-xs ${className}`}>
      {children}
    </Badge>
  );
}

function Stat({ icon: Icon, label, value, sub }) {
  return (
    <Card className="rounded-2xl">
      <CardContent className="p-5 min-h-[120px] flex items-center">
        <div className="flex w-full items-center justify-between gap-3">
          <div className="space-y-1">
            <div className="text-sm text-muted-foreground">{label}</div>
            <div className="text-2xl font-semibold tracking-tight">{value}</div>
            {sub ? <div className="text-xs text-muted-foreground">{sub}</div> : null}
          </div>
          <div className="h-10 w-10 rounded-2xl bg-muted flex items-center justify-center">
            <Icon className="h-5 w-5" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function SectionTitle({ kicker, title, desc, icon: Icon }) {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {Icon ? <Icon className="h-4 w-4" /> : null}
        <span className="uppercase tracking-wider">{kicker}</span>
      </div>
      <div className="text-2xl md:text-3xl font-semibold tracking-tight brand-serif">{title}</div>
      {desc ? <div className="text-sm md:text-base text-muted-foreground max-w-2xl">{desc}</div> : null}
    </div>
  );
}

function NativeSelect({ value, onChange, options, className = "" }) {
  return (
    <div className={`relative ${className}`}>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="h-10 w-full rounded-2xl border border-border bg-background px-3 pr-10 text-sm outline-none focus:ring-2 focus:ring-offset-2"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
      <Filter className="h-4 w-4 text-muted-foreground absolute right-3 top-3 pointer-events-none" />
    </div>
  );
}

function BrandLogo({ className = "", alt = `${BRAND.firm} logo` }) {
  return (
    <img
      src={BRAND.logoSrc}
      alt={alt}
      className={className}
      loading="eager"
      decoding="async"
      onError={(e) => {
        if (e.currentTarget.dataset.fallbackApplied === "1") return;
        e.currentTarget.dataset.fallbackApplied = "1";
        e.currentTarget.src = BRAND.logoFallbackSrc;
      }}
    />
  );
}

function Nav({ active, onNavigate }) {
  const items = [
    { key: "product", label: "About" },
    { key: "demo", label: "Dashboard" },
    { key: "explore", label: "Explore Data" },
  ];

  return (
    <div className="sticky top-0 z-40 backdrop-blur bg-white/95 border-b border-border">
      <div className="mx-auto max-w-6xl px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <BrandLogo className="h-10 md:h-12 w-auto" />
          <div className="leading-tight">
            <div className="font-semibold brand-serif tracking-wide">{BRAND.firm}</div>
            <div className="text-xs text-muted-foreground brand-caps">{BRAND.product}</div>
          </div>
        </div>
        <div className="hidden md:flex items-center gap-2">
          {items.map((it) => (
            <Button
              key={it.key}
              variant={active === it.key ? "secondary" : "ghost"}
              className="rounded-2xl"
              onClick={() => onNavigate(it.key)}
            >
              {it.label}
            </Button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <Button className="rounded-2xl" onClick={() => onNavigate("explore")}>
            Explore Data <ChevronRight className="h-4 w-4 ml-2" />
          </Button>
        </div>
      </div>
    </div>
  );
}

function Hero({ onNavigate }) {
  return (
    <div className="mx-auto max-w-3xl px-4 py-14 md:py-20 text-center">
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="space-y-6"
      >
        <div className="flex flex-col items-center">
          <div className="w-[240px] sm:w-[300px] md:w-[360px] h-[110px] sm:h-[140px] md:h-[170px] flex items-center justify-center">
            <BrandLogo className="w-full h-full object-contain" />
          </div>
        </div>
        <Pill className="w-fit mx-auto">SEC EDGAR 10-D filings • 40 deals • Auto ABS</Pill>
        <div className="text-4xl md:text-5xl font-semibold tracking-tight leading-tight brand-serif">
          {BRAND.product}: Auto ABS trigger monitoring from public filings.
        </div>
        <div className="text-base md:text-lg text-muted-foreground max-w-xl mx-auto">
          Every month, servicers file detailed loan performance data with the SEC. This tool parses those
          filings, tracks 60+ day delinquency against trigger thresholds, and shows where structural
          protection is eroding.
        </div>
        <div className="flex flex-col sm:flex-row gap-3 justify-center">
          <Button className="rounded-2xl" onClick={() => onNavigate("demo")}>
            View dashboard <ChevronRight className="h-4 w-4 ml-2" />
          </Button>
          <Button variant="secondary" className="rounded-2xl" onClick={() => onNavigate("explore")}>
            Explore the data
          </Button>
        </div>
        <div className="flex items-center justify-center gap-4 text-xs text-muted-foreground flex-wrap">
          <div className="flex items-center gap-1.5"><Layers className="h-3.5 w-3.5" /> 1,183 monthly snapshots</div>
          <div className="flex items-center gap-1.5"><Sparkles className="h-3.5 w-3.5" /> Explainable scoring</div>
        </div>
      </motion.div>
    </div>
  );
}

function FeatureGrid() {
  const features = [
    { icon: Layers, title: "Source: SEC EDGAR 10-D", desc: "Monthly ABS distribution reports filed with the SEC. Exhibit 99.1 contains the servicer report — pool balance, delinquency buckets, loss history." },
    { icon: LineChartIcon, title: "Cushion metric", desc: "Trigger cushion = (threshold − current 60+ DQ) / threshold. A value of 1.0 means fully buffered; 0 means at the line; negative means breached." },
    { icon: TrendingDown, title: "Risk scoring", desc: "Composite score from cushion level, 3-month trend, 6-month volatility, and macro regime (NY Fed household debt percentile)." },
    { icon: Bell, title: "Coverage", desc: "Prime and subprime auto ABS deals, 2020–2025 vintages. Santander Drive, AmeriCredit, Drive Auto, Honda, Toyota, Hyundai, and more." },
  ];

  return (
    <div className="mx-auto max-w-6xl px-4 py-12">
      <SectionTitle
        kicker="Methodology"
        title="How triggers are tracked"
        desc="DQ trigger cushion is computed directly from monthly servicer reports. No proxies, no estimates — just what the trustee published."
        icon={Gauge}
      />
      <div className="mt-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {features.map((f) => (
          <Card key={f.title} className="rounded-2xl">
            <CardContent className="p-6 pt-7 space-y-4">
              <div className="h-10 w-10 rounded-2xl bg-muted flex items-center justify-center mt-1">
                <f.icon className="h-5 w-5" />
              </div>
              <div className="font-semibold">{f.title}</div>
              <div className="text-sm text-muted-foreground">{f.desc}</div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

function DemoDashboard({ demo, dataStatus }) {
  const [query, setQuery] = useState("");
  const [riskFilter, setRiskFilter] = useState("all");
  const deals = useMemo(() => demo?.deals ?? [], [demo]);
  const [selectedDealId, setSelectedDealId] = useState(null);

  useEffect(() => {
    if (!deals.length) return;
    if (!selectedDealId || !deals.find((d) => d.dealId === selectedDealId)) {
      setSelectedDealId(deals[0].dealId);
    }
  }, [deals, selectedDealId]);

  const selected = useMemo(() => {
    if (!deals.length) return null;
    return deals.find((d) => d.dealId === selectedDealId) ?? deals[0];
  }, [selectedDealId, deals]);

  const rows = useMemo(() => {
    const flat = deals.flatMap((d) =>
      d.triggers.map((t) => ({
        dealId: d.dealId,
        cusip: d.cusip,
        collateral: d.collateral,
        tranche: d.tranche,
        geo: d.geo,
        macroTheme: d.macro.theme,
        macroPct: d.macro.percentile,
        macroRegime: d.macro.source ? percentRankLabel(d.macro.percentile) : "Normal",
        ...t,
      }))
    );

    const withData = flat.filter((r) =>
      r.metric &&
      r.current != null &&
      r.threshold != null &&
      r.cushion != null &&
      Number.isFinite(r.score)
    );

    const q = query.trim().toLowerCase();
    const filtered = withData.filter((r) => {
      const hit = !q
        ? true
        : [r.dealId, r.cusip, r.collateral, r.tranche, r.metric, r.triggerId]
            .join(" ")
            .toLowerCase()
            .includes(q);
      const passRisk = riskFilter === "all" ? true
        : riskFilter === "red" ? r.score >= 0.75
        : riskFilter === "yellow" ? r.score >= 0.45 && r.score < 0.75
        : riskFilter === "green" ? r.score < 0.45
        : true;
      return hit && passRisk;
    });

    return filtered.sort((a, b) => b.score - a.score);
  }, [deals, query, riskFilter]);

  if (!deals.length) {
    return (
      <div className="mx-auto max-w-6xl px-4 py-12">
        <Card className="rounded-2xl">
          <CardContent className="p-6">
            <div className="text-sm text-muted-foreground">
              {dataStatus === "loading" ? "Loading demo data…" : "No demo data loaded yet."}
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!selected) {
    return null;
  }
  const macroRegime = selected.macro?.source ? percentRankLabel(selected.macro.percentile) : "Normal";
  const collateralMetrics = selected.collateralMetrics ?? [];
  const cushionSeries = selected.cushionSeries ?? [];
  const dqSeries = selected.dqSeries ?? [];
  const cushionSample = cushionSeries[0] ?? {};
  const cushionTooltipLabel = (label, payload) => {
    const point = Array.isArray(payload) && payload.length ? payload[0]?.payload ?? {} : {};
    const source =
      point.dqThresholdSource ??
      point.ocThresholdSource ??
      point.icThresholdSource ??
      point.thresholdSource;
    const sourceText = thresholdSourceLabel(source);
    if (point.periodEnd) {
      return `${label} (${point.periodEnd}) • threshold: ${sourceText}`;
    }
    return `${label} • threshold: ${sourceText}`;
  };

  return (
    <div className="mx-auto max-w-6xl px-4 py-12">
      <div className="flex items-start justify-between gap-4 flex-col md:flex-row">
        <SectionTitle
          kicker="Dashboard"
          title="Ranked trigger deterioration"
          desc="Deals sorted by risk score. Click any row to see the cushion trajectory, delinquency chart, and scoring breakdown."
          icon={Bell}
        />
        <div className="flex items-center gap-2">
          <Button variant="secondary" className="rounded-2xl">
            <Download className="h-4 w-4 mr-2" /> Export
          </Button>
        </div>
      </div>

      <div className="mt-6 grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="rounded-2xl lg:col-span-2">
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between gap-3 flex-col md:flex-row">
              <div>
                <CardTitle className="text-base">Most at-risk triggers</CardTitle>
                <CardDescription>Sorted by risk score (explainable rule-based scoring)</CardDescription>
              </div>
              <div className="flex gap-2 w-full md:w-auto">
                <div className="relative w-full md:w-72">
                  <Search className="h-4 w-4 text-muted-foreground absolute left-3 top-3" />
                  <Input
                    className="pl-9 rounded-2xl"
                    placeholder="Search deal, CUSIP, metric…"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                </div>
                <NativeSelect
                  value={riskFilter}
                  onChange={setRiskFilter}
                  options={[
                    { value: "all", label: "All" },
                    { value: "red", label: "🔴 Red" },
                    { value: "yellow", label: "🟡 Yellow" },
                    { value: "green", label: "🟢 Green" },
                  ]}
                  className="w-[150px]"
                />
              </div>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-y-auto max-h-[420px]">
              <Table>
                <TableHeader className="sticky top-0 bg-background z-10">
                  <TableRow>
                    <TableHead>Deal</TableHead>
                    <TableHead>Tranche</TableHead>
                    <TableHead>Trigger</TableHead>
                    <TableHead className="text-right">Cushion</TableHead>
                    <TableHead className="text-right">Δ 3m</TableHead>
                    <TableHead className="text-right">Score</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((r) => {
                    const scoreColor = r.score >= 0.75 ? "#ef4444" : r.score >= 0.45 ? "#eab308" : "#22c55e";
                    return (
                      <TableRow key={`${r.dealId}-${r.triggerId}`} className="cursor-pointer" onClick={() => setSelectedDealId(r.dealId)}>
                        <TableCell>
                          <div className="font-medium">{r.dealId}</div>
                          <div className="text-xs text-muted-foreground">{r.collateral} • {r.cusip}</div>
                        </TableCell>
                        <TableCell>
                          <div className="font-medium">{r.tranche}</div>
                          <div className="text-xs text-muted-foreground">{r.geo}</div>
                        </TableCell>
                        <TableCell>
                          <div className="font-medium">{r.metric}</div>
                          <div className="text-xs text-muted-foreground">
                            {r.direction} {formatMetricValue(r.metric, r.threshold, r.threshold)} • current{" "}
                            {formatMetricValue(r.metric, r.current, r.threshold)}
                          </div>
                        </TableCell>
                        <TableCell className="text-right"><div className="font-medium">{fmtPct(r.cushion)}</div></TableCell>
                        <TableCell className="text-right">
                          <span className={r.change3m < 0 ? "text-destructive font-medium" : "text-muted-foreground"}>
                            {r.change3m != null ? fmtPct(r.change3m) : "—"}
                          </span>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center justify-end gap-2">
                            <div className="w-14 h-1.5 bg-muted rounded-full overflow-hidden">
                              <div className="h-full rounded-full" style={{ width: `${r.score * 100}%`, backgroundColor: scoreColor }} />
                            </div>
                            <span className="text-xs font-medium tabular-nums w-5 text-right">{Math.round(r.score * 100)}</span>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Selected deal</CardTitle>
            <CardDescription>{selected.dealId} • {selected.tranche}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-medium">Macro regime</div>
                <div className="text-xs text-muted-foreground">{selected.macro.series}</div>
              </div>
              <Pill tone={macroRegime === "Severe" ? "destructive" : macroRegime === "Moderate" ? "secondary" : "outline"}>
                {macroRegime} ({Math.round(selected.macro.percentile * 100)}th)
              </Pill>
            </div>
            <div>
              <div className="flex items-center justify-between mb-2">
                <div className="text-sm font-medium">Deal risk</div>
                <div className="text-xs text-muted-foreground">Max trigger score</div>
              </div>
              <Progress value={Math.round(Math.max(...selected.triggers.map((t) => t.score)) * 100)} />
            </div>
            <div className="space-y-2">
              <div className="text-sm font-medium">Why it’s flagged</div>
              <div className="text-sm text-muted-foreground leading-relaxed">{selected.explanation}</div>
            </div>
            <div className="space-y-1">
              <div className="text-sm font-medium">Threshold source</div>
              {selected.triggers.map((t) => (
                <div key={`${t.triggerId}-source`} className="text-xs text-muted-foreground">
                  {t.metric}: {thresholdSourceLabel(t.thresholdSource)}
                </div>
              ))}
              <div className="text-xs text-muted-foreground">Per date: hover the cushion chart.</div>
            </div>
            <Separator />
            <div className="grid grid-cols-2 gap-3">
              {collateralMetrics.map((m) => (
                <div key={m.name} className="rounded-2xl border border-border p-3">
                  <div className="text-xs text-muted-foreground">{m.name}</div>
                  <div className="mt-1 flex items-baseline justify-between">
                    <div className="text-lg font-semibold">{fmtPct(m.cur)}</div>
                    <div className="text-xs text-muted-foreground">{m.chg >= 0 ? "+" : ""}{fmtPct(m.chg)}</div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card className="rounded-2xl">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Trigger cushion over time</CardTitle>
            <CardDescription>OC/IC cushion (distance to threshold). Hover for threshold source by date.</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={cushionSeries} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="m" tickMargin={8} />
                <YAxis tickFormatter={(v) => `${Math.round(v * 100)}%`} />
                <Tooltip formatter={(v) => fmtPct(v)} labelFormatter={cushionTooltipLabel} />
                <Legend />
                {cushionSample.oc !== undefined ? <Line name="OC cushion" type="monotone" dataKey="oc" stroke="#3b82f6" strokeWidth={2} dot={false} /> : null}
                {cushionSample.ic !== undefined ? <Line name="IC cushion" type="monotone" dataKey="ic" stroke="#8b5cf6" strokeWidth={2} dot={false} /> : null}
                {cushionSample.dq !== undefined ? <Line name="DQ cushion" type="monotone" dataKey="dq" stroke="#ef4444" strokeWidth={2} dot={false} /> : null}
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Collateral deterioration</CardTitle>
            <CardDescription>60+ delinquency rate (trend)</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dqSeries} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="m" tickMargin={8} />
                <YAxis tickFormatter={(v) => `${Math.round(v * 100)}%`} />
                <Tooltip formatter={(v) => fmtPct(v)} />
                <Bar name="60+ DQ" dataKey="dq60" fill="#ef4444" />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

    </div>
  );
}


// ─── ExploreData section ─────────────────────────────────────────────────────

function ExploreData({ data }) {
  const [selectedDealId, setSelectedDealId] = useState(null);
  const [tierFilter, setTierFilter] = useState("all");
  const [sortBy, setSortBy] = useState("currentBreach");

  const dealsWithBreachMetrics = useMemo(() => {
    if (!data) return [];
    return data.deals.map((d) => {
      const series = Array.isArray(d.series) ? d.series : [];
      const n = Number.isFinite(d.n) ? d.n : series.length;
      const currentBreachCount = series.filter((point) => Number.isFinite(point?.cushion) && point.cushion < 0).length;
      const currentBreachRate = n > 0 ? currentBreachCount / n : 0;
      const forwardBreachCount = Number.isFinite(d.breachCount)
        ? d.breachCount
        : series.filter((point) => Boolean(point?.breach)).length;
      const forwardBreachRate = Number.isFinite(d.breachRate)
        ? d.breachRate
        : (n > 0 ? forwardBreachCount / n : 0);
      return {
        ...d,
        n,
        currentBreachCount,
        currentBreachRate,
        forwardBreachCount,
        forwardBreachRate,
      };
    });
  }, [data]);

  const defaultDeal = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return null;
    return (
      dealsWithBreachMetrics.find((d) => d.currentBreachRate > 0 && d.tier === "subprime") ||
      dealsWithBreachMetrics.find((d) => d.forwardBreachRate > 0 && d.tier === "subprime") ||
      dealsWithBreachMetrics[0]
    );
  }, [dealsWithBreachMetrics]);

  const selectedDeal = useMemo(
    () => dealsWithBreachMetrics.find((d) => d.dealId === selectedDealId) || defaultDeal,
    [dealsWithBreachMetrics, selectedDealId, defaultDeal],
  );
  const monthsBelowThreshold = useMemo(() => {
    if (!selectedDeal?.series) return 0;
    return selectedDeal.series.filter((point) => Number.isFinite(point?.cushion) && point.cushion < 0).length;
  }, [selectedDeal]);

  const vintageChartData = useMemo(() => {
    if (!data) return [];
    const byYear = {};
    data.vintageChart.forEach((row) => {
      if (row.vintage < "2021" || row.vintage > "2024") return;
      if (!byYear[row.vintage]) byYear[row.vintage] = { vintage: row.vintage };
      byYear[row.vintage][row.tier] = +(row.breachRate * 100).toFixed(1);
      byYear[row.vintage][`${row.tier}N`] = row.n;
    });
    return Object.values(byYear).sort((a, b) => a.vintage.localeCompare(b.vintage));
  }, [data]);

  const filteredDeals = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return [];
    let ds = tierFilter === "all" ? dealsWithBreachMetrics : dealsWithBreachMetrics.filter((d) => d.tier === tierFilter);
    return [...ds].sort((a, b) => {
      if (sortBy === "currentBreach") return b.currentBreachRate - a.currentBreachRate;
      if (sortBy === "forwardBreach") return b.forwardBreachRate - a.forwardBreachRate;
      if (sortBy === "vintage") return a.vintage.localeCompare(b.vintage);
      return a.dealId.localeCompare(b.dealId);
    });
  }, [dealsWithBreachMetrics, tierFilter, sortBy]);

  const primeBreachPct = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return 0;
    const prime = dealsWithBreachMetrics.filter((d) => d.tier === "prime");
    const num = prime.reduce((s, d) => s + d.currentBreachCount, 0);
    const den = prime.reduce((s, d) => s + d.n, 0);
    return den > 0 ? num / den : 0;
  }, [dealsWithBreachMetrics]);

  const primeForwardBreachPct = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return 0;
    const prime = dealsWithBreachMetrics.filter((d) => d.tier === "prime");
    const num = prime.reduce((s, d) => s + d.forwardBreachCount, 0);
    const den = prime.reduce((s, d) => s + d.n, 0);
    return den > 0 ? num / den : 0;
  }, [dealsWithBreachMetrics]);

  const subBreachPct = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return 0;
    const sub = dealsWithBreachMetrics.filter((d) => d.tier === "subprime");
    const num = sub.reduce((s, d) => s + d.currentBreachCount, 0);
    const den = sub.reduce((s, d) => s + d.n, 0);
    return den > 0 ? num / den : 0;
  }, [dealsWithBreachMetrics]);

  const subForwardBreachPct = useMemo(() => {
    if (!dealsWithBreachMetrics.length) return 0;
    const sub = dealsWithBreachMetrics.filter((d) => d.tier === "subprime");
    const num = sub.reduce((s, d) => s + d.forwardBreachCount, 0);
    const den = sub.reduce((s, d) => s + d.n, 0);
    return den > 0 ? num / den : 0;
  }, [dealsWithBreachMetrics]);

  const shortDealLabel = (id) =>
    id
      .replace(" Auto Securitization Trust", "")
      .replace(" Auto Receivables Trust", "")
      .replace(" Receivables Owner Trust", "")
      .replace(" Auto Receivables Owner Trust", "")
      .replace(" Lending", "");

  if (!data) {
    return <div className="py-24 text-center text-sm text-muted-foreground">Loading data…</div>;
  }

  return (
    <div className="mx-auto max-w-6xl px-4 py-16 space-y-12">
      <SectionTitle
        kicker="Real SEC Data"
        icon={LineChartIcon}
        title="Explore the data"
        desc={`${data.totalDeals} auto ABS deals · ${data.totalRows.toLocaleString()} servicer report snapshots pulled directly from SEC EDGAR 10-D filings.`}
      />

      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Stat
          icon={Layers}
          label="Deals tracked"
          value={data.totalDeals}
          sub={`${data.totalRows.toLocaleString()} monthly snapshots`}
        />
        <Stat
          icon={CheckCircle2}
          label="Prime current breach"
          value={fmtPct(primeBreachPct)}
          sub={`Forward 6m label: ${fmtPct(primeForwardBreachPct)}`}
        />
        <Stat
          icon={AlertTriangle}
          label="Subprime current breach"
          value={fmtPct(subBreachPct)}
          sub={`Forward 6m label: ${fmtPct(subForwardBreachPct)}`}
        />
      </div>

      {/* Vintage chart */}
      <Card className="rounded-3xl">
        <CardHeader>
          <CardTitle className="text-base">Forward 6m breach label by vintage — prime vs subprime</CardTitle>
          <CardDescription>
            % of months that breach within the next 6 filings (forward-looking label)
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={vintageChartData} barCategoryGap="35%" barGap={4}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="vintage" tick={{ fontSize: 12 }} />
              <YAxis tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} domain={[0, "auto"]} />
              <Tooltip
                formatter={(v, name) => [`${Number(v).toFixed(1)}%`, name === "prime" ? "Prime" : "Subprime"]}
                contentStyle={{ borderRadius: "12px", fontSize: "12px" }}
              />
              <Legend formatter={(v) => (v === "prime" ? "Prime" : "Subprime")} />
              <Bar dataKey="prime" name="prime" fill="#94a3b8" radius={[4, 4, 0, 0]} />
              <Bar dataKey="subprime" name="subprime" fill="#ef4444" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-muted-foreground mt-3">
            2021–2022 subprime vintages: 0% breach. 2023 jumped to 27.5%, 2024 to 48.5%. Prime held at 0–2% throughout.
          </p>
        </CardContent>
      </Card>

      {/* Cushion timeline */}
      <Card className="rounded-3xl">
        <CardHeader>
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div>
              <CardTitle className="text-base">Trigger cushion over time</CardTitle>
              <CardDescription>1.0 = fully buffered · 0 = at threshold · negative = breached. Red dots = cushion below 0.</CardDescription>
            </div>
            <select
              className="h-9 rounded-2xl border border-border bg-background px-3 text-sm outline-none shrink-0 max-w-xs"
              value={selectedDeal?.dealId || ""}
              onChange={(e) => setSelectedDealId(e.target.value)}
            >
              {data.deals.map((d) => (
                <option key={d.dealId} value={d.dealId}>
                  {shortDealLabel(d.dealId)}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          {selectedDeal && (
            <>
              <div className="flex gap-2 mb-4 flex-wrap">
                <Pill tone={selectedDeal.tier === "subprime" ? "destructive" : "outline"}>
                  {selectedDeal.tier}
                </Pill>
                <Pill tone="outline">Vintage {selectedDeal.vintage}</Pill>
                <Pill tone={monthsBelowThreshold > 0 ? "destructive" : "outline"}>
                  {monthsBelowThreshold}/{selectedDeal.series.length} months below threshold
                </Pill>
                <Pill tone={selectedDeal.forwardBreachCount > 0 ? "destructive" : "outline"}>
                  {selectedDeal.forwardBreachCount}/{selectedDeal.n} forward 6m breach labels
                </Pill>
              </div>
              <ResponsiveContainer width="100%" height={230}>
                <LineChart data={selectedDeal.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="p" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
                  <YAxis
                    tickFormatter={(v) => `${(v * 100).toFixed(0)}%`}
                    tick={{ fontSize: 10 }}
                    domain={[-1.5, 1.1]}
                  />
                  <Tooltip
                    formatter={(v) => [`${(v * 100).toFixed(1)}%`, "Cushion"]}
                    contentStyle={{ borderRadius: "12px", fontSize: "12px" }}
                    labelFormatter={(l) => `Period: ${l}`}
                  />
                  <ReferenceLine
                    y={0}
                    stroke="#ef4444"
                    strokeDasharray="4 4"
                    label={{ value: "Threshold", position: "insideTopLeft", fontSize: 10, fill: "#ef4444" }}
                  />
                  <Line
                    type="monotone"
                    dataKey="cushion"
                    stroke="#1d4ed8"
                    strokeWidth={2}
                    dot={(props) => {
                      const { cx, cy, payload, key } = props;
                      const isBreached = Number.isFinite(payload?.cushion) && payload.cushion < 0;
                      return <circle key={key} cx={cx} cy={cy} r={3} fill={isBreached ? "#ef4444" : "#1d4ed8"} stroke="none" />;
                    }}
                    activeDot={{ r: 5 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </>
          )}
        </CardContent>
      </Card>

      {/* Deal table — scrollable */}
      <Card className="rounded-3xl">
        <CardHeader>
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <CardTitle className="text-base">All tracked deals</CardTitle>
            <div className="flex gap-2 flex-wrap">
              <NativeSelect
                value={tierFilter}
                onChange={setTierFilter}
                options={[
                  { value: "all", label: "All tiers" },
                  { value: "prime", label: "Prime" },
                  { value: "subprime", label: "Subprime" },
                ]}
                className="w-36"
              />
                <NativeSelect
                  value={sortBy}
                  onChange={setSortBy}
                  options={[
                    { value: "currentBreach", label: "Current breach ↓" },
                    { value: "forwardBreach", label: "Forward 6m ↓" },
                    { value: "vintage", label: "Vintage" },
                    { value: "name", label: "Name" },
                  ]}
                  className="w-40"
                />
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-y-auto max-h-[520px]">
            <Table>
              <TableHeader className="sticky top-0 bg-background z-10">
                <TableRow>
                  <TableHead>Deal</TableHead>
                  <TableHead>Tier</TableHead>
                  <TableHead>Vintage</TableHead>
                  <TableHead className="text-right">Months</TableHead>
                  <TableHead className="text-right">Current breach</TableHead>
                  <TableHead className="text-right">Forward 6m</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredDeals.map((d) => (
                  <TableRow
                    key={d.dealId}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => setSelectedDealId(d.dealId)}
                  >
                    <TableCell className="font-medium text-sm">{d.dealId}</TableCell>
                    <TableCell>
                      <Pill tone={d.tier === "subprime" ? "secondary" : "outline"} className="text-[10px]">
                        {d.tier}
                      </Pill>
                    </TableCell>
                    <TableCell className="text-sm">{d.vintage}</TableCell>
                    <TableCell className="text-right text-sm text-muted-foreground">{d.n}</TableCell>
                    <TableCell className="text-right">
                      <span
                        className={
                          d.currentBreachRate > 0.1
                            ? "text-destructive font-semibold"
                            : d.currentBreachRate > 0
                              ? "text-yellow-600 font-medium"
                              : "text-muted-foreground"
                        }
                      >
                        {fmtPct(d.currentBreachRate)}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">
                      <span
                        className={
                          d.forwardBreachRate > 0.1
                            ? "text-destructive font-semibold"
                            : d.forwardBreachRate > 0
                              ? "text-yellow-600 font-medium"
                              : "text-muted-foreground"
                        }
                      >
                        {fmtPct(d.forwardBreachRate)}
                      </span>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <p className="text-xs text-muted-foreground">
        Source: SEC EDGAR 10-D filings, Exhibit 99.1. Subprime trigger thresholds estimated from servicer reports or
        industry norms where not explicitly stated. Data as of {data.asOf}. Not investment advice.
      </p>
    </div>
  );
}

// ─── Main app ─────────────────────────────────────────────────────────────────

export default function TriggerMonitorWebsiteDemo() {
  const [active, setActive] = useState("product");
  const [demo, setDemo] = useState(EMPTY_DEMO);
  const [dataStatus, setDataStatus] = useState("loading");
  const [explorerData, setExplorerData] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      const data = await loadDemoData();
      if (cancelled) return;
      if (data) {
        setDemo(data);
        setDataStatus("loaded");
      } else {
        setDataStatus("error");
      }
    };
    run();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    let cancelled = false;
    loadExplorerData().then((data) => {
      if (!cancelled && data) setExplorerData(data);
    });
    return () => { cancelled = true; };
  }, []);

  const handleNavigate = (key) => {
    setActive(key);
    const target = document.getElementById(`section-${key}`);
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Nav active={active} onNavigate={handleNavigate} />
      <section id="section-product" className="bg-white">
        <Hero onNavigate={handleNavigate} />
        <FeatureGrid />
      </section>

      <section id="section-demo" className="bg-[#f9f7f3]">
        <DemoDashboard demo={demo} dataStatus={dataStatus} />
      </section>
      <section id="section-explore" className="bg-[#f2efea]">
        <ExploreData data={explorerData} />
      </section>

      <footer className="border-t border-border bg-[#f2efea]">
        <div className="mx-auto max-w-6xl px-4 py-8 flex flex-col md:flex-row gap-3 items-start md:items-center justify-between">
          <div className="flex items-center gap-4">
            <BrandLogo className="h-10 md:h-12 w-auto" />
            <div className="text-sm text-muted-foreground brand-serif">{BRAND.firm} • {BRAND.product}</div>
          </div>
          <div className="text-xs text-muted-foreground">Built on SEC EDGAR 10-D filings. Not investment advice.</div>
        </div>
      </footer>
    </div>
  );
}
