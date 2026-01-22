// ==UserScript==
// @name         Merrill Portfolio Beta
// @namespace    mailto:lixinjun@umich.edu
// @version      0.2.0
// @description  Aggregates holdings from multiple tables (Equities, MFs, Accounts), sums Market Values, and estimates Beta.
// @match        https://*.ml.com/TFPHoldings/*
// @grant        GM_xmlhttpRequest
// @connect      stooq.com
// @connect      query1.finance.yahoo.com
// @run-at       document-end
// ==/UserScript==

(() => {
    "use strict";

    // --- Configuration ---
    const DEFAULT_LOOKBACK_DAYS = 252;
    const DEFAULT_MARKET_STOOQ = "spy.us";
    const DEFAULT_MARKET_YAHOO = "SPY";
    const CACHE_PREFIX = "mb_beta_cache_v2_";
    const CACHE_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

    // --- API URL Generators ---
    const STOOQ_URL = (symbol) =>
`https://stooq.com/q/d/l/?s=${encodeURIComponent(symbol)}&i=d`;

    const YAHOO_URL = (symbol) =>
`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=2y&interval=1d&events=history`;

    // --- Caching ---
    function getCachedBeta(source, symbol, market, lookback) {
        const key = `${CACHE_PREFIX}${source}_${symbol}_${market}_${lookback}`;
        try {
            const raw = localStorage.getItem(key);
            if (!raw) return null;
            const data = JSON.parse(raw);
            if (Date.now() - data.timestamp > CACHE_DURATION_MS) {
                localStorage.removeItem(key);
                return null;
            }
            return data;
        } catch (e) { return null; }
    }

    function setCachedBeta(source, symbol, market, lookback, beta, n) {
        const key = `${CACHE_PREFIX}${source}_${symbol}_${market}_${lookback}`;
        try {
            localStorage.setItem(key, JSON.stringify({ beta, n, timestamp: Date.now() }));
        } catch (e) { console.warn("Cache full", e); }
    }

    // --- Network ---
    function gmFetch(url) {
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                method: "GET", url, timeout: 20000,
                onload: (res) => (res.status >= 200 && res.status < 300) ? resolve(res.responseText) : reject(new Error(`HTTP ${res.status}`)),
                ontimeout: () => reject(new Error("Timeout")),
                onerror: () => reject(new Error("Network Error")),
            });
        });
    }

    // --- Data Fetching ---
    async function fetchPricesStooq(symbol) {
        const txt = await gmFetch(STOOQ_URL(symbol));
        const lines = txt.trim().split(/\r?\n/);
        if (lines.length < 3) throw new Error("No data");
        const header = lines[0].split(",");
        const dateIdx = header.findIndex(h => h.toLowerCase() === "date");
        const closeIdx = header.findIndex(h => h.toLowerCase() === "close");
        const rows = [];
        for (let i = 1; i < lines.length; i++) {
            const parts = lines[i].split(",");
            if (parts[dateIdx] && !isNaN(parts[closeIdx])) rows.push({ date: parts[dateIdx], close: Number(parts[closeIdx]) });
        }
        return rows.sort((a, b) => (a.date < b.date ? -1 : 1));
    }

    async function fetchPricesYahoo(symbol) {
        const jsonTxt = await gmFetch(YAHOO_URL(symbol));
        const data = JSON.parse(jsonTxt);
        const result = data.chart?.result?.[0];
        if (!result?.timestamp || !result?.indicators?.quote?.[0]) throw new Error("Invalid Yahoo JSON");
        const timestamps = result.timestamp;
        const adjClose = result.indicators.adjclose?.[0]?.adjclose || result.indicators.quote[0].close;
        const rows = [];
        for (let i = 0; i < timestamps.length; i++) {
            if (timestamps[i] && Number.isFinite(adjClose[i])) {
                rows.push({ date: new Date(timestamps[i] * 1000).toISOString().split("T")[0], close: adjClose[i] });
            }
        }
        return rows;
    }

    // --- Math ---
    function toReturns(rows) {
        const rets = [];
        for (let i = 1; i < rows.length; i++) {
            const p0 = rows[i - 1].close, p1 = rows[i].close;
            if (p0 > 0 && p1 > 0) rets.push({ date: rows[i].date, r: p1 / p0 - 1 });
        }
        return rets;
    }

    function alignReturns(asset, mkt) {
        const mktMap = new Map(mkt.map(x => [x.date, x.r]));
        const xs = [], ys = [];
        for (const a of asset) {
            const m = mktMap.get(a.date);
            if (m !== undefined) { xs.push(a.r); ys.push(m); }
        }
        return { asset: xs, mkt: ys };
    }

    function calculateBeta(asset, mkt) {
        const n = asset.length;
        if (n < 20) return NaN;
        const meanA = asset.reduce((a, b) => a + b, 0) / n;
        const meanM = mkt.reduce((a, b) => a + b, 0) / n;
        let cov = 0, varM = 0;
        for (let i = 0; i < n; i++) {
            cov += (asset[i] - meanA) * (mkt[i] - meanM);
            varM += (mkt[i] - meanM) * (mkt[i] - meanM);
        }
        return varM === 0 ? NaN : cov / varM;
    }

    function normalizeTicker(raw, source) {
        let t = (raw || "").trim().toUpperCase();
        if (!t) return "";
        if (source === "stooq") {
            if (/\.[a-z]{2,}$/i.test(t)) return t.toLowerCase();
            t = t.replace(/\s+/g, "");
            return t.includes(".") ? t.toLowerCase() : `${t.toLowerCase()}.us`;
        } else {
            return t.replace(/\./g, "-");
        }
    }

    function safeNum(x) {
        const n = Number(String(x).replace(/[$,%]/g, "").trim());
        return Number.isFinite(n) ? n : NaN;
    }

    function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

    // --- UI ---
    const panel = document.createElement("div");
    // UPDATED: Use Flexbox (display: flex) and hidden overflow on the container
    panel.style.cssText = `
position: fixed; right: 20px; bottom: 20px; width: 460px;
max-height: 80vh; z-index: 999999;
background: #ffffff; color: #111111; border: 1px solid #cfcfcf;
border-radius: 10px; box-shadow: 0 8px 24px rgba(0,0,0,0.18);
font: 12px/1.35 system-ui, sans-serif;
display: flex; flex-direction: column; /* Stacks header and content */
overflow: hidden; /* Prevents double scrollbars */
resize: both; /* Enables the resize handle in bottom-right corner */
`;

    panel.innerHTML = `
<div id="mb_header" style="padding:10px 12px; border-bottom:1px solid #eee; display:flex; gap:8px; align-items:center; background:#f9f9f9; border-radius:10px 10px 0 0; cursor: move; user-select: none; flex: 0 0 auto;">
  <div style="font-weight:700; font-size:13px; flex:1;">Merrill Portfolio Beta</div>
  <button id="mb_close" style="cursor:pointer; background:transparent; border:none; font-size:16px; color:#555;">✕</button>
</div>

<div style="padding:12px; display:grid; gap:12px; overflow-y: auto; flex: 1;">

  <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:8px; grid-auto-rows: min-content;">
    <label style="display:grid; gap:4px;">
       <div style="opacity:.8; font-size:11px;">Source</div>
       <select id="mb_source" style="padding:5px; border-radius:6px; border:1px solid #ccc;">
         <option value="yahoo">Yahoo</option>
         <option value="stooq">Stooq</option>
       </select>
    </label>
    <label style="display:grid; gap:4px;">
      <div style="opacity:.8; font-size:11px;">Proxy</div>
      <input id="mb_market" value="${DEFAULT_MARKET_YAHOO}" style="padding:6px 8px; border-radius:6px; border:1px solid #ccc;">
    </label>
    <label style="display:grid; gap:4px;">
      <div style="opacity:.8; font-size:11px;">Lookback</div>
      <input id="mb_lookback" value="${DEFAULT_LOOKBACK_DAYS}" style="padding:6px 8px; border-radius:6px; border:1px solid #ccc;">
    </label>
  </div>

  <div>
    <div style="opacity:.8; margin-bottom:6px; font-size:11px;">Holdings (TICKER, WEIGHT)</div>
    <textarea id="mb_holdings" rows="6" style="width:100%; padding:8px; border-radius:6px; border:1px solid #ccc; font-family:monospace;" placeholder="AAPL, 25%&#10;MSFT, 0.25"></textarea>
  </div>

  <div>
    <button id="mb_scrape" style="cursor:pointer; background:#eef; color:#333; border:1px solid #ccd; border-radius:6px; padding:6px 12px;flex: 1;white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">Auto-scrape & Aggregate</button>
    <button id="mb_calc" style="cursor:pointer; background:#0055a5; color:#fff; border:none; border-radius:6px; padding:6px 12px; font-weight:600;">Calculate</button>
    <button id="mb_clear" style="cursor:pointer; background:#fff; color:#333; border:1px solid #ccc; border-radius:6px; padding:6px 12px;">Clear</button>
  </div>

  <div id="mb_status" style="white-space:pre-wrap; font-family:monospace; font-size:11px; color:#444; min-height:1.2em;"></div>
  <div id="mb_tablewrap"></div>
</div>
`;
    document.documentElement.appendChild(panel);

    const $ = (id) => panel.querySelector(id);
    $("#mb_close").addEventListener("click", () => panel.remove());
    $("#mb_clear").addEventListener("click", () => {
        $("#mb_holdings").value = "";
        $("#mb_status").textContent = "";
        $("#mb_tablewrap").innerHTML = "";
    });

    $("#mb_source").addEventListener("change", (e) => {
        $("#mb_market").value = (e.target.value === "stooq") ? DEFAULT_MARKET_STOOQ : DEFAULT_MARKET_YAHOO;
    });

    // --- Draggable ---
    const header = $("#mb_header");
    let isDragging = false, startX, startY, initialLeft, initialTop;
    header.addEventListener("mousedown", (e) => {
        isDragging = true; startX = e.clientX; startY = e.clientY;
        const rect = panel.getBoundingClientRect();
        initialLeft = rect.left; initialTop = rect.top;
        panel.style.left = `${initialLeft}px`; panel.style.top = `${initialTop}px`;
        panel.style.right = "auto"; panel.style.bottom = "auto";
        document.addEventListener("mousemove", onMouseMove);
        document.addEventListener("mouseup", onMouseUp);
    });
    function onMouseMove(e) { if(isDragging) { panel.style.left = `${initialLeft + (e.clientX-startX)}px`; panel.style.top = `${initialTop + (e.clientY-startY)}px`; } }
    function onMouseUp() { isDragging = false; document.removeEventListener("mousemove", onMouseMove); document.removeEventListener("mouseup", onMouseUp); }

    // --- Aggregation Logic ---
    $("#mb_scrape").addEventListener("click", () => {
        const result = scrapeAndAggregate();
        if (!result.holdings.length) {
            $("#mb_status").textContent = "No valid holdings found in any table.";
            return;
        }
        $("#mb_holdings").value = result.holdings.map(h => `${h.ticker}, ${h.weight}`).join("\n");
        $("#mb_status").textContent = `Found ${result.tableCount} tables.\nAggregated ${result.rowCount} rows into ${result.holdings.length} unique holdings.\nTotal Value Detected: $${result.totalMV.toLocaleString(undefined, {maximumFractionDigits:0})}`;
    });

    function scrapeAndAggregate() {
        const tables = Array.from(document.querySelectorAll("table"));
        let rawRows = [];
        let tableCount = 0;

        for (const table of tables) {
            // 1. Check if table looks like a holdings table
            const text = table.innerText || "";
            if (!/symbol|ticker/i.test(text)) continue;

            const headers = Array.from(table.querySelectorAll("thead th, tr th")).map(th => (th.innerText || "").trim());

            // Identify Columns
            const symIdx = headers.findIndex(h => /symbol|ticker|asset/i.test(h));
            // "Value" often appears as "Market Value", "Current Value", "Amount", etc.
            const mvIdx = headers.findIndex(h => /market\s*value|current\s*value|value/i.test(h));
            // Fallback: Percentage Weight
            const wIdx = headers.findIndex(h => /weight|%|allocation/i.test(h));

            if (symIdx === -1) continue; // Must have a ticker column
            // Must have either a value or a weight column to be useful
            if (mvIdx === -1 && wIdx === -1) continue;

            const rows = Array.from(table.querySelectorAll("tbody tr"));
            if (rows.length < 1) continue;

            let foundInTable = 0;
            for (const tr of rows) {
                const tds = Array.from(tr.querySelectorAll("td"));
                if (tds.length <= Math.max(symIdx, mvIdx, wIdx)) continue;

                // Extract Ticker
                let ticker = "";
                const a = tds[symIdx].querySelector("a");
                if (a) ticker = a.textContent.trim().split(" ")[0];

                if (!ticker || /[a-z0-9]/.test(ticker)) continue; // skip junk/lowercase

                // Extract Market Value ($)
                let mv = 0;
                if (mvIdx !== -1) {
                    mv = safeNum(tds[mvIdx].innerText);
                }

                // Extract Weight (%)
                let w = 0;
                if (wIdx !== -1) {
                    const rawW = tds[wIdx].innerText;
                    w = safeNum(rawW);
                    if (/%/.test(rawW)) w /= 100;
                }

                // We only care if we have MV or W
                if (mv > 0 || w > 0) {
                    rawRows.push({ ticker, mv, w });
                    foundInTable++;
                }
            }
            if (foundInTable > 0) tableCount++;
        }

        // --- Aggregation Step ---
        // Prefer Market Value for aggregation.
        // If some rows have MV and others don't, it's messy. We will assume if MV exists, we use it.

        // Map: Ticker -> Total MV
        const tickerMap = new Map();
        let globalMV = 0;

        for (const r of rawRows) {
            if (r.mv > 0) {
                const cur = tickerMap.get(r.ticker) || 0;
                tickerMap.set(r.ticker, cur + r.mv);
                globalMV += r.mv;
            }
        }

        // If we found valid dollar values, use them to build weights
        if (globalMV > 0) {
            const uniqueHoldings = [];
            for (const [ticker, totalMV] of tickerMap.entries()) {
                uniqueHoldings.push({ ticker, weight: (totalMV / globalMV).toFixed(5) }); // 5 decimals for precision
            }
            return { tableCount, rowCount: rawRows.length, holdings: uniqueHoldings, totalMV: globalMV };
        }

        // Fallback: If no dollar values found (only %), we can't mathematically sum them accurately
        // without knowing the account size.
        // Best Effort: Just average the weights or sum them (assuming they belong to the same pot).
        // Here we will just List them all and let the user decide.
        // But likely we won't hit this on Merrill, as MV is standard.
        return {
            tableCount,
            rowCount: rawRows.length,
            holdings: rawRows.map(r => ({ ticker: r.ticker, weight: r.w || 0.01 })), // default dummy weight
            totalMV: 0
        };
    }

    // --- Calculation & Render (Same as before) ---
    $("#mb_calc").addEventListener("click", async () => {
        const source = $("#mb_source").value;
        const marketSymRaw = $("#mb_market").value;
        const marketSym = normalizeTicker(marketSymRaw, source);
        const lookback = parseInt($("#mb_lookback").value) || DEFAULT_LOOKBACK_DAYS;

        $("#mb_status").textContent = `Preparing (${source.toUpperCase()})...`;
        $("#mb_tablewrap").innerHTML = "";

        try {
            const lines = $("#mb_holdings").value.split("\n").filter(x => x.trim());
            const holdings = [];
            lines.forEach(line => {
                const [t, wRaw] = line.split(",").map(s => s.trim());
                let w = safeNum(wRaw);
                if (/%/.test(wRaw)) w /= 100;
                if (t && w > 0) holdings.push({ ticker: t, weight: w });
            });

            if (!holdings.length) throw new Error("No holdings.");
            const wSum = holdings.reduce((s, h) => s + h.weight, 0);
            holdings.forEach(h => h.weight /= wSum);

            const needsMarket = holdings.some(h => !getCachedBeta(source, normalizeTicker(h.ticker, source), marketSym, lookback));
            let marketRets = null;
            if (needsMarket) {
                $("#mb_status").textContent = `Fetching Market: ${marketSym}`;
                const mPrices = source === "stooq" ? await fetchPricesStooq(marketSym) : await fetchPricesYahoo(marketSym);
                marketRets = toReturns(mPrices.slice(-(lookback + 10)));
            }

            const results = [];
            let i = 0;
            for (const h of holdings) {
                i++;
                const sym = normalizeTicker(h.ticker, source);
                const cached = getCachedBeta(source, sym, marketSym, lookback);
                if (cached) { results.push({ ...h, beta: cached.beta, n: cached.n, cached: true }); continue; }

                $("#mb_status").textContent = `Fetching ${i}/${holdings.length}: ${sym}`;
                try {
                    if (i > 1) await sleep(1000);
                    const prices = source === "stooq" ? await fetchPricesStooq(sym) : await fetchPricesYahoo(sym);
                    const assetRets = toReturns(prices.slice(-(lookback + 10)));
                    const { asset, mkt } = alignReturns(assetRets, marketRets);
                    const beta = calculateBeta(asset, mkt);
                    if (!isNaN(beta)) setCachedBeta(source, sym, marketSym, lookback, beta, asset.length);
                    results.push({ ...h, beta, n: asset.length, cached: false });
                } catch (e) {
                    results.push({ ...h, beta: NaN, n: 0, error: true });
                }
            }
            renderTable(results);
            $("#mb_status").textContent = `Done. Source: ${source}`;
        } catch (e) { $("#mb_status").textContent = `Error: ${e.message}`; }
    });

    function renderTable(results) {
        results.sort((a, b) => b.weight - a.weight);
        const table = document.createElement("table");
        table.style.cssText = "width:100%; border-collapse:collapse; margin-top:8px; font-size:12px;";
        table.innerHTML = `
  <thead>
    <tr style="background:#f4f4f4; border-bottom:1px solid #ddd;">
      <th style="text-align:left; padding:6px;">Ticker</th>
      <th style="text-align:right; padding:6px;">Weight</th>
      <th style="text-align:right; padding:6px;">Beta ✎</th>
      <th style="text-align:right; padding:6px;">w×β</th>
    </tr>
  </thead>
  <tbody></tbody>
  <tfoot>
    <tr style="border-top:2px solid #ccc; font-weight:700; background:#fafafa;">
      <td style="padding:8px;">Portfolio</td>
      <td style="padding:8px;">100%</td>
      <td style="padding:8px;"></td>
      <td style="padding:8px; text-align:right;" id="mb_total"></td>
    </tr>
  </tfoot>`;

    const tbody = table.querySelector("tbody");
    results.forEach(r => {
        const tr = document.createElement("tr");
        tr.style.borderBottom = "1px solid #eee";
        tr.innerHTML = `
    <td style="padding:6px;"><b>${r.ticker}</b> <span style="font-size:9px; color:#999;">${r.cached ? 'cached' : ''}</span></td>
    <td style="padding:6px; text-align:right;" data-w="${r.weight}">${(r.weight*100).toFixed(1)}%</td>
    <td style="padding:6px; text-align:right;"><input type="number" step="0.01" class="b-in" value="${isNaN(r.beta)?'':r.beta.toFixed(2)}" style="width:50px; text-align:right;"></td>
    <td style="padding:6px; text-align:right;" class="wb-out">—</td>`;
      tbody.appendChild(tr);
  });
    $("#mb_tablewrap").innerHTML = ""; $("#mb_tablewrap").appendChild(table);

    const recalc = () => {
        let sum = 0, wTotal = 0;
        tbody.querySelectorAll("tr").forEach(tr => {
            const w = parseFloat(tr.querySelector("td[data-w]").dataset.w);
            const b = parseFloat(tr.querySelector(".b-in").value);
            if (!isNaN(b)) { sum += w * b; wTotal += w; tr.querySelector(".wb-out").textContent = (w*b).toFixed(3); }
            else tr.querySelector(".wb-out").textContent = "—";
        });
        $("#mb_total").textContent = wTotal > 0 ? sum.toFixed(3) : "—";
    };
    tbody.addEventListener("input", recalc); recalc();
}
})();
