from __future__ import annotations

import argparse
import csv
import html
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CT = timezone(timedelta(hours=-5))

# Mapping of teams to contract identifiers
TEAM_MAP = {
    "padres": {
        "name": "San Diego Padres",
        "polymarket_token": "21397422589879803522488375172913562805167469503053305050103496090884867406737",
        "kalshi_ticker": "KXMLBGAME-26MAY291845SDWSH-SD"
    },
    "nationals": {
        "name": "Washington Nationals",
        "polymarket_token": "8589315888960875332332797137454929625257904729097387308223061136689771454702",
        "kalshi_ticker": "KXMLBGAME-26MAY291845SDWSH-WSH"
    }
}


def parse_local_ts(value: str) -> float:
    try:
        val = float(value)
        if val > 1000000000000:  # ms
            return val / 1000.0
        return val
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            if value.endswith("Z") or "+" in value:
                clean_val = value.replace("Z", "+00:00")
                return datetime.fromisoformat(clean_val).timestamp()
            return datetime.strptime(value, fmt).replace(tzinfo=CT).timestamp()
        except ValueError:
            pass
    raise ValueError(f"unsupported timestamp: {value!r}")


def ts_label(ts: float) -> str:
    return datetime.fromtimestamp(ts, CT).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def decimal_val(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed / 100.0 if parsed > 1.0 else parsed


def best_price(levels: Any, bid: bool) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = [decimal_val(level.get("price")) for level in levels if isinstance(level, dict)]
    prices = [price for price in prices if price is not None]
    if not prices:
        return None
    return max(prices) if bid else min(prices)


def first_value(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = values.get(key)
        if value is not None and value != "":
            return value
    return None


def iter_jsonl(paths: list[Path]):
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def load_raw_poly(raw_dir: Path, token_id: str, start_ts: float, end_ts: float) -> list[dict]:
    rows = []
    paths = [raw_dir / "raw_polymarket_20260529.jsonl", raw_dir / "raw_polymarket_20260530.jsonl"]
    
    for envelope in iter_jsonl(paths):
        received_ms = envelope.get("received_at_ms")
        if received_ms is None:
            continue
        ts = received_ms / 1000.0
        if ts < start_ts or ts >= end_ts:
            continue
            
        payload = envelope.get("payload")
        if not payload:
            continue
            
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            
            event_type = node.get("event_type", "")
            if event_type == "price_change":
                for change in node.get("price_changes") or []:
                    if str(change.get("asset_id")) == token_id:
                        bid = decimal_val(change.get("best_bid"))
                        ask = decimal_val(change.get("best_ask"))
                        last = decimal_val(change.get("price"))
                        if bid is not None and ask is not None:
                            rows.append({
                                "ts": ts,
                                "ts_ms": received_ms,
                                "mid": (bid + ask) / 2.0,
                                "best_bid": bid,
                                "best_ask": ask,
                                "last_trade": last
                            })
            else:
                asset_id = str(node.get("asset_id") or "")
                if asset_id != token_id:
                    continue
                
                bid, ask, last = None, None, None
                if event_type == "book":
                    bid = best_price(node.get("bids"), True)
                    ask = best_price(node.get("asks"), False)
                elif event_type == "best_bid_ask":
                    bid = decimal_val(node.get("best_bid"))
                    ask = decimal_val(node.get("best_ask"))
                elif event_type == "last_trade_price":
                    last = decimal_val(node.get("price"))
                    
                if bid is not None or ask is not None or last is not None:
                    mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None
                    rows.append({
                        "ts": ts,
                        "ts_ms": received_ms,
                        "mid": mid,
                        "best_bid": bid,
                        "best_ask": ask,
                        "last_trade": last
                    })
    return sorted(rows, key=lambda row: row["ts"])


def load_raw_kalshi(raw_dir: Path, ticker: str, start_ts: float, end_ts: float) -> list[dict]:
    rows = []
    paths = [raw_dir / "raw_kalshi_20260529.jsonl", raw_dir / "raw_kalshi_20260530.jsonl"]
    
    for envelope in iter_jsonl(paths):
        received_ms = envelope.get("received_at_ms")
        if received_ms is None:
            continue
        ts = received_ms / 1000.0
        if ts < start_ts or ts >= end_ts:
            continue
            
        payload = envelope.get("payload") or {}
        if payload.get("type") != "ticker":
            continue
            
        msg = payload.get("msg") or {}
        if msg.get("market_ticker") != ticker:
            continue
            
        yes_bid = decimal_val(first_value(msg, "yes_bid_dollars", "yes_bid"))
        yes_ask = decimal_val(first_value(msg, "yes_ask_dollars", "yes_ask"))
        last = decimal_val(first_value(msg, "price_dollars", "price", "last_price_dollars"))
        
        if yes_bid is not None or yes_ask is not None:
            mid = (yes_bid + yes_ask) / 2.0 if (yes_bid is not None and yes_ask is not None) else None
            rows.append({
                "ts": ts,
                "ts_ms": received_ms,
                "mid": mid,
                "best_bid": yes_bid,
                "best_ask": yes_ask,
                "last_trade": last
            })
    return sorted(rows, key=lambda row: row["ts"])


def write_points_csv(path: Path, poly: list[dict], kalshi: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "series",
                "received_ts",
                "received_ms",
                "received_local",
                "mid",
                "best_bid",
                "best_ask",
                "last_trade",
            ],
        )
        writer.writeheader()
        for row in poly:
            writer.writerow({
                "series": "polymarket",
                "received_ts": f"{row['ts']:.6f}",
                "received_ms": row["ts_ms"],
                "received_local": ts_label(row["ts"]),
                "mid": row["mid"],
                "best_bid": row["best_bid"],
                "best_ask": row["best_ask"],
                "last_trade": row["last_trade"],
            })
        for row in kalshi:
            writer.writerow({
                "series": "kalshi",
                "received_ts": f"{row['ts']:.6f}",
                "received_ms": row["ts_ms"],
                "received_local": ts_label(row["ts"]),
                "mid": row["mid"],
                "best_bid": row["best_bid"],
                "best_ask": row["best_ask"],
                "last_trade": row["last_trade"],
            })


def write_html(path: Path, title: str, poly_points: list[dict], kalshi_points: list[dict], meta: dict) -> None:
    payload = json.dumps(
        {
            "poly": poly_points,
            "kalshi": kalshi_points,
            "meta": meta,
        },
        separators=(",", ":"),
    )
    path.write_text(
        f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: #121214; color: #e4e4e7; }}
    #bar {{ padding: 12px 18px; border-bottom: 1px solid #27272a; background: #18181b; position: sticky; top: 0; z-index: 2; display: flex; align-items: center; justify-content: space-between; }}
    #title-section {{ display: flex; flex-direction: column; }}
    #title-text {{ font-size: 16px; font-weight: 600; color: #f4f4f5; }}
    #chart {{ width: 100vw; height: calc(100vh - 84px); display: block; background: #09090b; cursor: crosshair; }}
    #tooltip {{ position: fixed; display: none; pointer-events: none; background: rgba(24, 24, 27, 0.95); border: 1px solid #3f3f46; border-radius: 6px; padding: 10px 12px; font-size: 13px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); white-space: nowrap; color: #f4f4f5; }}
    button {{ background: #2563eb; color: #ffffff; border: none; border-radius: 4px; padding: 6px 14px; font-size: 13px; font-weight: 500; cursor: pointer; transition: background 0.15s; }}
    button:hover {{ background: #1d4ed8; }}
    .hint {{ color: #a1a1aa; font-size: 12px; margin-top: 4px; }}
    #readout {{ font-family: monospace; color: #38bdf8; font-size: 13px; margin-left: 12px; }}
    .controls {{ display: flex; align-items: center; }}
  </style>
</head>
<body>
  <div id="bar">
    <div id="title-section">
      <span id="title-text">{html.escape(title)}</span>
      <div class="hint">Wheel = zoom (around cursor); drag = pan; x-axis = time from start in milliseconds; blue = Polymarket Mid; red = Kalshi Mid. Accurate to milliseconds.</div>
    </div>
    <div class="controls">
      <span id="readout"></span>
      <button id="reset" style="margin-left: 16px;">Reset View</button>
    </div>
  </div>
  <canvas id="chart"></canvas>
  <div id="tooltip"></div>
  <script>
const DATA = {payload};
const canvas = document.getElementById('chart');
const ctx = canvas.getContext('2d');
const readout = document.getElementById('readout');
const tooltip = document.getElementById('tooltip');
const pad = {{l: 80, r: 40, t: 40, b: 50}};
let all = DATA.poly.concat(DATA.kalshi);
let x0 = 0, x1 = DATA.meta.duration_ms;
let fullX0 = x0, fullX1 = x1;
let y0 = DATA.meta.y_min, y1 = DATA.meta.y_max;
let fullY0 = y0, fullY1 = y1;
let dragging = false, last = null;

function resize() {{
  canvas.width = Math.floor(canvas.clientWidth * devicePixelRatio);
  canvas.height = Math.floor(canvas.clientHeight * devicePixelRatio);
  draw();
}}
function sx(x) {{ return pad.l + (x - x0) / (x1 - x0) * (canvas.width / devicePixelRatio - pad.l - pad.r); }}
function sy(y) {{ return pad.t + (y1 - y) / (y1 - y0) * (canvas.height / devicePixelRatio - pad.t - pad.b); }}
function ix(px) {{ return x0 + (px - pad.l) / (canvas.width / devicePixelRatio - pad.l - pad.r) * (x1 - x0); }}
function iy(py) {{ return y1 - (py - pad.t) / (canvas.height / devicePixelRatio - pad.t - pad.b) * (y1 - y0); }}

function fmtElapsed(ms) {{
  const sign = ms < 0 ? '-' : '';
  ms = Math.abs(ms);
  const whole = Math.floor(ms / 1000);
  const frac = Math.floor(ms % 1000).toString().padStart(3, '0');
  return sign + whole + '.' + frac + 's';
}}

function nearestPoint(mx, my) {{
  let best = null;
  let bestD = Infinity;
  for (const p of all) {{
    if (p.x < x0 || p.x > x1) continue;
    const dx = sx(p.x) - mx;
    const dy = sy(p.y) - my;
    const d = dx * dx + dy * dy;
    if (d < bestD) {{ bestD = d; best = p; }}
  }}
  return bestD <= 150 ? best : null;
}}

function drawLine(points, color, width) {{
  ctx.beginPath();
  let started = false;
  for (const p of points) {{
    if (p.x < x0 || p.x > x1) continue;
    const x = sx(p.x), y = sy(p.y);
    if (!started) {{ ctx.moveTo(x, y); started = true; }} else {{ ctx.lineTo(x, y); }}
  }}
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.stroke();
}}

function drawPoints(points, color, radius) {{
  ctx.fillStyle = color;
  for (const p of points) {{
    if (p.x < x0 || p.x > x1) continue;
    ctx.beginPath();
    ctx.arc(sx(p.x), sy(p.y), radius, 0, Math.PI * 2);
    ctx.fill();
  }}
}}

function drawGrid() {{
  const w = canvas.width / devicePixelRatio, h = canvas.height / devicePixelRatio;
  ctx.strokeStyle = '#27272a'; ctx.lineWidth = 1;
  ctx.fillStyle = '#a1a1aa'; ctx.font = '11px monospace';
  
  // Draw vertical grid lines
  for (let i = 0; i <= 10; i++) {{
    const x = pad.l + i / 10 * (w - pad.l - pad.r);
    ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, h - pad.b); ctx.stroke();
    ctx.fillText(fmtElapsed(ix(x)), x - 20, h - 24);
  }}
  
  // Draw horizontal grid lines
  for (let i = 0; i <= 10; i++) {{
    const y = pad.t + i / 10 * (h - pad.t - pad.b);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
    ctx.fillText('$' + iy(y).toFixed(3), 16, y + 4);
  }}
}}

function drawLegend() {{
  ctx.font = '12px Arial';
  ctx.fillStyle = '#38bdf8'; ctx.fillRect(pad.l, 14, 18, 4);
  ctx.fillStyle = '#f4f4f5'; ctx.fillText('Polymarket YES Mid ($)', pad.l + 24, 18);
  
  ctx.fillStyle = '#ef4444'; ctx.fillRect(pad.l + 220, 14, 18, 4);
  ctx.fillStyle = '#f4f4f5'; ctx.fillText('Kalshi YES Mid ($)', pad.l + 244, 18);
}}

function draw() {{
  ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
  ctx.clearRect(0, 0, canvas.width / devicePixelRatio, canvas.height / devicePixelRatio);
  drawGrid();
  
  // Draw lines
  drawLine(DATA.poly, '#38bdf8', 1.8);
  drawLine(DATA.kalshi, '#ef4444', 1.8);
  
  // Draw dots
  drawPoints(DATA.poly, 'rgba(56, 189, 248, 0.4)', 2.5);
  drawPoints(DATA.kalshi, 'rgba(239, 68, 68, 0.4)', 2.5);
  
  drawLegend();
}}

canvas.addEventListener('wheel', (e) => {{
  e.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  const cx = ix(mx), cy = iy(my);
  const f = e.deltaY < 0 ? 0.85 : 1.18;
  x0 = cx - (cx - x0) * f; x1 = cx + (x1 - cx) * f;
  y0 = cy - (cy - y0) * f; y1 = cy + (y1 - cy) * f;
  draw();
}}, {{passive:false}});

canvas.addEventListener('mousedown', e => {{ 
  dragging = true; 
  last = {{x:e.clientX, y:e.clientY}}; 
}});
window.addEventListener('mouseup', () => {{ dragging = false; }});
window.addEventListener('mousemove', e => {{
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left, my = e.clientY - rect.top;
  readout.textContent = 'Cursor: +' + fmtElapsed(ix(mx)) + ' / Value: $' + iy(my).toFixed(4);
  
  const near = nearestPoint(mx, my);
  if (near) {{
    tooltip.style.display = 'block';
    tooltip.style.left = (e.clientX + 14) + 'px';
    tooltip.style.top = (e.clientY + 14) + 'px';
    tooltip.innerHTML =
      '<span style="font-weight:bold; color:' + (near.series === 'Polymarket' ? '#38bdf8' : '#ef4444') + '">' + near.series + '</span><br>' +
      'Elapsed: +' + fmtElapsed(near.x) + '<br>' +
      'Local Time: ' + near.local + '<br>' +
      'Exact TS: ' + near.received_ts + '<br>' +
      'Mid Price: $' + near.y.toFixed(4) + '<br>' +
      'Bid: $' + (near.bid ? near.bid.toFixed(3) : 'N/A') + ' | Ask: $' + (near.ask ? near.ask.toFixed(3) : 'N/A') + '<br>' +
      'Last Trade: ' + (near.last ? '$' + near.last.toFixed(3) : 'N/A');
  }} else {{
    tooltip.style.display = 'none';
  }}
  
  if (!dragging) return;
  const w = canvas.width / devicePixelRatio - pad.l - pad.r;
  const h = canvas.height / devicePixelRatio - pad.t - pad.b;
  const dx = (e.clientX - last.x) / w * (x1 - x0);
  const dy = (e.clientY - last.y) / h * (y1 - y0);
  x0 -= dx; x1 -= dx; y0 += dy; y1 += dy;
  last = {{x:e.clientX, y:e.clientY}};
  draw();
}});

document.getElementById('reset').onclick = () => {{ 
  x0 = fullX0; x1 = fullX1; y0 = fullY0; y1 = fullY1; 
  draw(); 
}};
canvas.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});
window.addEventListener('resize', resize);
resize();
  </script>
</body>
</html>
""",
        encoding="utf-8",
    )


def build_window(raw_dir: Path, start_ts: float, duration: float, team: str, out_dir: Path) -> dict:
    end_ts = start_ts + duration
    team_info = TEAM_MAP[team]
    
    poly = load_raw_poly(raw_dir, team_info["polymarket_token"], start_ts, end_ts)
    kalshi = load_raw_kalshi(raw_dir, team_info["kalshi_ticker"], start_ts, end_ts)
    
    if not poly and not kalshi:
        raise RuntimeError(f"missing raw points for {ts_label(start_ts)}: poly={len(poly)}, kalshi={len(kalshi)}")
        
    start_ms = int(round(start_ts * 1000))
    end_ms = int(round(end_ts * 1000))
    
    p_points = [
        {
            "x": row["ts_ms"] - start_ms,
            "epoch_ms": row["ts_ms"],
            "received_ts": f"{row['ts']:.6f}",
            "local": ts_label(row["ts"]),
            "y": row["mid"],
            "bid": row["best_bid"],
            "ask": row["best_ask"],
            "last": row["last_trade"],
            "series": "Polymarket",
        }
        for row in poly if row["mid"] is not None
    ]
    
    k_points = [
        {
            "x": row["ts_ms"] - start_ms,
            "epoch_ms": row["ts_ms"],
            "received_ts": f"{row['ts']:.6f}",
            "local": ts_label(row["ts"]),
            "y": row["mid"],
            "bid": row["best_bid"],
            "ask": row["best_ask"],
            "last": row["last_trade"],
            "series": "Kalshi",
        }
        for row in kalshi if row["mid"] is not None
    ]
    
    all_y = [p["y"] for p in p_points + k_points]
    if all_y:
        y_min = max(min(all_y) - 0.05, 0.0)
        y_max = min(max(all_y) + 0.05, 1.0)
    else:
        y_min = 0.0
        y_max = 1.0
        
    label = datetime.fromtimestamp(start_ts, CT).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    html_path = out_dir / f"poly_kalshi_comparison_{team}_{label}.html"
    csv_path = out_dir / f"poly_kalshi_comparison_{team}_{label}.csv"
    
    title = f"Polymarket vs Kalshi YES Mid Price | Team: {team_info['name']} | Window Start: {ts_label(start_ts)} CT"
    
    meta = {
        "start_ms": start_ms,
        "end_ms": end_ms,
        "duration_ms": end_ms - start_ms,
        "y_min": y_min,
        "y_max": y_max,
    }
    
    write_html(html_path, title, p_points, k_points, meta)
    write_points_csv(csv_path, poly, kalshi)
    
    return {
        "team": team,
        "window_start": ts_label(start_ts),
        "window_end": ts_label(end_ts),
        "polymarket_points": len(poly),
        "kalshi_points": len(kalshi),
        "y_min": y_min,
        "y_max": y_max,
        "html": str(html_path),
        "csv": str(csv_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Polymarket vs Kalshi sports price curves.")
    parser.add_argument("--raw-dir", default=str(ROOT))
    parser.add_argument("--team", choices=["padres", "nationals"], default="padres")
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--windows", nargs="+", default=["2026-05-29 20:42:00.000"])
    parser.add_argument("--out-dir", default=str(ROOT))
    parser.add_argument("--game-start", required=True)
    parser.add_argument("--game-end", required=True)
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

 #   summaries = []
 #   for window in args.windows:
 #       summaries.append(build_window(raw_dir, parse_local_ts(window), args.duration_seconds, args.team, out_dir))
    summary = build_window(
        raw_dir=raw_dir,
        start_ts=parse_local_ts(args.game_start),
        duration=parse_local_ts(args.game_end) - parse_local_ts(args.game_start),
        team=args.team,
        out_dir=out_dir,
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))

 #   for row in summaries:
 #       print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
