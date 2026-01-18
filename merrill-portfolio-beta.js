// ==UserScript==
// @name         Merrill Portfolio Beta (via Stooq historical beta)
// @namespace    https://example.com/
// @version      0.1.0
// @description  Estimates per-holding beta vs SPY using Stooq daily closes, then computes portfolio beta (weight * beta).
// @match        https://*.merrilledge.com/*
// @match        https://*.ml.com/*
// @grant        GM_xmlhttpRequest
// @connect      stooq.com
// @run-at       document-end
// ==/UserScript==

(() => {
    "use strict";

    /***********************
   * What this script does
   * - Pulls daily close prices from Stooq CSV
   * - Computes beta = Cov(r_asset, r_mkt) / Var(r_mkt)
   * - Uses SPY as market proxy by default (spy.us)
   * - Portfolio beta = Σ(weight_i * beta_i)
   ***********************/

    const DEFAULT_LOOKBACK_DAYS = 252; // ~1 trading year
    const DEFAULT_MARKET = "spy.us";   // market proxy
    const STOOQ_DAILY_URL = (symbol) =>
    `https://stooq.com/q/d/l/?s=${encodeURIComponent(symbol)}&i=d`;

    // --- Small helpers ---
    function gmFetchText(url, timeoutMs = 20000) {
        function formatAnyError(e) {
            if (!e) return "Unknown error";
            if (e instanceof Error) return e.message || String(e);

            // GM_xmlhttpRequest error objects are often plain objects
            try {
                return JSON.stringify(e, Object.getOwnPropertyNames(e), 2);
            } catch {
                return String(e);
            }
        }
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                method: "GET",
                url,
                timeout: timeoutMs,

                onload: (res) => {
                    // res: {status, statusText, responseText, finalUrl, ...}
                    if (res.status >= 200 && res.status < 300) {
                        resolve(res.responseText);
                        return;
                    }

                    // Build a meaningful error
                    const bodyPreview = (res.responseText || "").slice(0, 200).replace(/\s+/g, " ").trim();
                    reject(
                        new Error(
                            `HTTP ${res.status} ${res.statusText || ""} for ${url}` +
                            (bodyPreview ? ` | Body: ${bodyPreview}` : "")
                        )
                    );
                },

                ontimeout: () => reject(new Error(`Timeout after ${timeoutMs}ms for ${url}`)),

                onerror: (e) => reject(new Error(`Network/GM error for ${url}: ${formatAnyError(e)}`)),
            });
        });
    }


    function parseStooqCsv(csvText) {
        // Stooq format: Date,Open,High,Low,Close,Volume
        // Example header: Date,Open,High,Low,Close,Volume
        const lines = csvText.trim().split(/\r?\n/);
        if (lines.length < 3) return [];
        const header = lines[0].split(",");
        const dateIdx = header.findIndex((h) => h.toLowerCase() === "date");
        const closeIdx = header.findIndex((h) => h.toLowerCase() === "close");
        if (dateIdx === -1 || closeIdx === -1) return [];

        const rows = [];
        for (let i = 1; i < lines.length; i++) {
            const parts = lines[i].split(",");
            const date = parts[dateIdx];
            const close = Number(parts[closeIdx]);
            if (!date || !Number.isFinite(close)) continue;
            rows.push({ date, close });
        }
        // Stooq returns ascending by date already, but ensure:
        rows.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
        return rows;
    }

    function toReturns(priceRows) {
        // simple daily returns r_t = close_t / close_{t-1} - 1
        const rets = [];
        for (let i = 1; i < priceRows.length; i++) {
            const p0 = priceRows[i - 1].close;
            const p1 = priceRows[i].close;
            if (!Number.isFinite(p0) || !Number.isFinite(p1) || p0 <= 0) continue;
            rets.push({ date: priceRows[i].date, r: p1 / p0 - 1 });
        }
        return rets;
    }

    function alignReturns(assetRets, mktRets) {
        // Align by date intersection
        const mktMap = new Map(mktRets.map((x) => [x.date, x.r]));
        const xs = [];
        const ys = [];
        for (const a of assetRets) {
            const m = mktMap.get(a.date);
            if (m === undefined) continue;
            xs.push(a.r);
            ys.push(m);
        }
        return { asset: xs, mkt: ys };
    }

    function variance(arr) {
        if (arr.length < 2) return NaN;
        const mean = arr.reduce((s, x) => s + x, 0) / arr.length;
        let v = 0;
        for (const x of arr) v += (x - mean) * (x - mean);
        return v / (arr.length - 1);
    }

    function covariance(a, b) {
        if (a.length !== b.length || a.length < 2) return NaN;
        const meanA = a.reduce((s, x) => s + x, 0) / a.length;
        const meanB = b.reduce((s, x) => s + x, 0) / b.length;
        let c = 0;
        for (let i = 0; i < a.length; i++) c += (a[i] - meanA) * (b[i] - meanB);
        return c / (a.length - 1);
    }

    function betaFromReturns(assetR, mktR) {
        const v = variance(mktR);
        if (!Number.isFinite(v) || v === 0) return NaN;
        const c = covariance(assetR, mktR);
        return c / v;
    }

    function normalizeToStooqSymbol(rawTicker) {
        // Convert common US tickers to stooq symbols:
        // AAPL -> aapl.us
        // BRK.B -> brk.b.us
        // Remove whitespace, keep dot for class shares.
        let t = (rawTicker || "").trim();
        if (!t) return "";
        // If user already typed ".us" or another stooq suffix, keep it (lowercase it).
        if (/\.[a-z]{2,}$/i.test(t)) return t.toLowerCase();
        // If they typed something like "AAPL US" or "AAPL.US", normalize:
        t = t.replace(/\s+/g, "");
        // Convert AAPL.US -> aapl.us
        if (t.includes(".")) return t.toLowerCase(); // already dotted (BRK.B)
        return `${t.toLowerCase()}.us`;
    }

    function safeNum(x) {
        const n = Number(String(x).replace(/[$,%]/g, "").trim());
        return Number.isFinite(n) ? n : NaN;
    }

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // --- UI ---
    const panel = document.createElement("div");
    panel.style.cssText = `
  position: fixed;
  right: 14px;
  bottom: 14px;
  width: 420px;
  max-height: 70vh;
  overflow: auto;
  z-index: 999999;
  background: #ffffff;
  color: #111111;
  border: 1px solid #cfcfcf;
  border-radius: 10px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.18);
  font: 12px/1.35 system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
  `;

    panel.innerHTML = `
    <div style="padding:10px 12px; border-bottom:1px solid #333; display:flex; gap:8px; align-items:center;">
      <div style="font-weight:700; font-size:13px; flex:1;">Portfolio Beta (Stooq)</div>
      <button id="mb_close" style="cursor:pointer; background:#f7f7f7; color:#111111; border:1px solid #444; border-radius:8px; padding:4px 8px;">✕</button>
    </div>

    <div style="padding:10px 12px; display:grid; gap:10px;">
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
        <label style="display:grid; gap:4px;">
          <div style="opacity:.8;">Market proxy (Stooq symbol)</div>
          <input id="mb_market" value="${DEFAULT_MARKET}" style="padding:6px 8px; border-radius:8px; border:1px solid #d0d0d0; background:#f7f7f7; color:#111111;">
        </label>
        <label style="display:grid; gap:4px;">
          <div style="opacity:.8;">Lookback (trading days)</div>
          <input id="mb_lookback" value="${DEFAULT_LOOKBACK_DAYS}" style="padding:6px 8px; border-radius:8px; border:1px solid #d0d0d0; background:#f7f7f7; color:#111111;">
        </label>
      </div>

      <div>
        <div style="opacity:.8; margin-bottom:6px;">
          Holdings input (one per line): <span style="opacity:.9;">TICKER, WEIGHT</span> (weight can be % or decimal)
        </div>
        <textarea id="mb_holdings" rows="6" style="width:100%; padding:8px; border-radius:8px; border:1px solid #d0d0d0; background:#f7f7f7; color:#111111;"
          placeholder="AAPL, 25%
MSFT, 25%
VOO, 50%"></textarea>
        <div style="margin-top:6px; opacity:.7;">
          Tip: weights don’t need to sum to 100%; we’ll normalize them.
        </div>
      </div>

      <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <button id="mb_scrape" style="cursor:pointer; background:#f7f7f7; color:#111111; border:0 solid #d0d0d0; border-radius:8px; padding:6px 10px;">Try auto-scrape</button>
        <button id="mb_calc" style="cursor:pointer; background:#f7f7f7; color:#111111; border:0; border-radius:8px; padding:6px 10px; font-weight:700;">Calculate beta</button>
        <button id="mb_clear" style="cursor:pointer; background:#f7f7f7; color:#111111; border:0 solid #d0d0d0; border-radius:8px; padding:6px 10px;">Clear</button>
      </div>

      <div id="mb_status" style="white-space:pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace; background:#f7f7f7; border:1px solid #d0d0d0; border-radius:8px; padding:8px; min-height:70px;"></div>

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

    // --- Auto-scrape best-effort ---
    $("#mb_scrape").addEventListener("click", () => {
        const scraped = scrapeHoldingsFromPage();
        if (!scraped.length) {
            $("#mb_status").textContent =
                "Auto-scrape: couldn’t confidently find holdings on this page.\n" +
                "Paste manually as: TICKER, WEIGHT (or use market value and we’ll compute weights).\n\n" +
                "If you want, open your Holdings table view and try again.";
            return;
        }
        const lines = scraped.map((h) => `${h.ticker}, ${h.weight}`).join("\n");
        $("#mb_holdings").value = lines;
        $("#mb_status").textContent = `Auto-scrape: found ${scraped.length} rows. Review weights/tickers before calculating.`;
    });

    function scrapeHoldingsFromPage() {
        // This is intentionally conservative and generic. It tries common patterns:
        // - a table containing the word "Symbol" or "Ticker"
        // - and a column that looks like a weight (%), or market value to derive weights
        const tables = Array.from(document.querySelectorAll("table"));
        let best = null;

        for (const table of tables) {
            const text = table.innerText || "";
            if (!/symbol|ticker/i.test(text)) continue;

            const headers = Array.from(table.querySelectorAll("thead th")).map((th) =>
                                                                               (th.innerText || "").trim()
                                                                              );
            const hasSymbolCol = headers.some((h) => /symbol|ticker/i.test(h));
            if (!hasSymbolCol) continue;

            const rows = Array.from(table.querySelectorAll("tbody tr"));
            if (rows.length < 2) continue;

            // Find indexes
            const symIdx = headers.findIndex((h) => /symbol|ticker/i.test(h));
            const weightIdx = headers.findIndex((h) => /weight|%|allocation/i.test(h));
            const mvIdx = headers.findIndex((h) => /market\s*value|value/i.test(h));

            const parsed = [];
            let totalMV = 0;

            for (const tr of rows) {
                const tds = Array.from(tr.querySelectorAll("td"));
                if (tds.length < Math.max(symIdx, weightIdx, mvIdx) + 1) continue;

                //const rawSym = (tds[symIdx]?.innerText || "").trim();
                //if (!rawSym || rawSym.length > 12) continue; // crude sanity check
                //const ticker = rawSym.replace(/[^A-Za-z0-9.\-]/g, "");
                const ticker = extractTickerFromCell(tds[symIdx]);
                if (!ticker || /[a-z0-9]/.test(ticker)) continue;


                let w = NaN;
                if (weightIdx !== -1) {
                    const rawW = (tds[weightIdx]?.innerText || "").trim();
                    w = safeNum(rawW);
                    if (Number.isFinite(w) && /%/.test(rawW)) w = w / 100;
                }

                let mv = NaN;
                if (!Number.isFinite(w) && mvIdx !== -1) {
                    const rawMV = (tds[mvIdx]?.innerText || "").trim();
                    mv = safeNum(rawMV);
                    if (Number.isFinite(mv)) {
                        totalMV += mv;
                    }
                }

                parsed.push({ ticker, weight: w, mv });
            }

            // If weights missing but market values exist, compute weights
            if (parsed.length && parsed.some((x) => Number.isFinite(x.mv)) && totalMV > 0) {
                for (const p of parsed) {
                    if (!Number.isFinite(p.weight) && Number.isFinite(p.mv)) p.weight = p.mv / totalMV;
                }
            }

            // Keep only those with a numeric weight
            const cleaned = parsed.filter((x) => x.ticker && Number.isFinite(x.weight) && x.weight > 0);
            if (!cleaned.length) continue;

            // Pick the biggest result set
            if (!best || cleaned.length > best.length) best = cleaned;
        }

        return (best || []).map((x) => ({
            ticker: x.ticker,
            weight: `${(x.weight * 100).toFixed(2)}%`,
        }));
    }

    // --- Main calculation ---
    $("#mb_calc").addEventListener("click", async () => {
        $("#mb_status").textContent = "Working…";
        $("#mb_tablewrap").innerHTML = "";

        try {
            const marketSym = normalizeToStooqSymbol($("#mb_market").value) || DEFAULT_MARKET;
            const lookback = Math.max(60, Math.min(1500, Math.floor(safeNum($("#mb_lookback").value) || DEFAULT_LOOKBACK_DAYS)));

            const holdings = parseHoldings($("#mb_holdings").value);
            if (!holdings.length) {
                $("#mb_status").textContent =
                    "No holdings parsed.\nEnter lines like:\nAAPL, 25%\nMSFT, 0.25\nVOO, 50%";
                return;
            }

            // Normalize weights to sum to 1
            const wSum = holdings.reduce((s, h) => s + h.weight, 0);
            if (!(wSum > 0)) throw new Error("Weights sum to 0.");
            holdings.forEach((h) => (h.weight = h.weight / wSum));

            $("#mb_status").textContent =
                `Fetching market (${marketSym}) + ${holdings.length} holdings from Stooq…\n` +
                `Lookback: ${lookback} trading days\n`;

            const marketPrices = await fetchPrices(marketSym);
            const marketSlice = marketPrices.slice(- (lookback + 5)); // +5 cushion for return calc
            const marketRets = toReturns(marketSlice);

            const results = [];
            for (const h of holdings) {
                const sym = normalizeToStooqSymbol(h.ticker);
                if (!sym) continue;

                $("#mb_status").textContent += `• ${h.ticker} → ${sym}\n`;

                const assetPrices = await fetchPrices(sym);
                const assetSlice = assetPrices.slice(- (lookback + 5));
                const assetRets = toReturns(assetSlice);

                const aligned = alignReturns(assetRets, marketRets);
                const beta = betaFromReturns(aligned.asset, aligned.mkt);

                results.push({
                    ticker: h.ticker,
                    stooq: sym,
                    weight: h.weight,
                    beta,
                    n: Math.min(aligned.asset.length, aligned.mkt.length),
                });
            }

            // Portfolio beta
            const portfolioBeta = results.reduce((s, r) => {
                const b = Number.isFinite(r.beta) ? r.beta : 0;
                return s + r.weight * b;
            }, 0);

            renderResults(results, portfolioBeta, marketSym);
        } catch (err) {
            $("#mb_status").textContent = `Error: ${err?.message || String(err)}`;
        }
    });

    function parseHoldings(text) {
        // Lines: TICKER, WEIGHT
        // WEIGHT: "25%" or "0.25" or "$1234" (not supported here)
        const lines = (text || "")
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter(Boolean);

        const out = [];
        for (const line of lines) {
            const parts = line.split(",").map((p) => p.trim());
            if (parts.length < 2) continue;
            const ticker = parts[0];
            const rawW = parts[1];
            let w = safeNum(rawW);
            if (!Number.isFinite(w)) continue;
            if (/%/.test(rawW)) w = w / 100;
            if (!(w > 0)) continue;
            out.push({ ticker, weight: w });
        }
        return out;
    }

    function extractTickerFromCell(td) {
        if (!td) return "";

        // 1) If there is a link, it's usually the real ticker
        const a = td.querySelector("a");
        if (a && a.textContent) {
            return a.textContent.trim().split(" ")[0];
        }

        // 2) Otherwise, take only the first text node (before popup/tooltip text)
        for (const node of td.childNodes) {
            if (node.nodeType === Node.TEXT_NODE) {
                const t = node.textContent;
                if (t) return t;
            }
        }

        // 3) Fallback: regex extract ticker-like token
        const txt = (td.textContent || "").trim();
        const m = txt.match(/[A-Z]{1,5}(?:\.[A-Z])?/);
        return m ? m[0] : "";
    }

    async function fetchPrices(symbol) {
        const url = STOOQ_DAILY_URL(symbol);
        const txt = await gmFetchText(url);
        const rows = parseStooqCsv(txt);
        if (!rows.length) {
            throw new Error(`No price data from Stooq for ${symbol}. Check symbol format (e.g., aapl.us).`);
        }
        await sleep(1000);
        return rows;
    }

    function renderResults(results, portfolioBeta, marketSym) {
        const bad = results.filter((r) => !Number.isFinite(r.beta) || r.n < 60);
        const good = results.filter((r) => Number.isFinite(r.beta) && r.n >= 60);

        const lines = [];
        lines.push(`Done.`);
        lines.push(`Market proxy: ${marketSym}`);
        lines.push(`Portfolio beta ≈ ${Number.isFinite(portfolioBeta) ? portfolioBeta.toFixed(3) : "NaN"}`);
        if (bad.length) {
            lines.push("");
            lines.push(`Warnings (${bad.length}): some betas may be unreliable (missing data or short overlap).`);
        }
        $("#mb_status").textContent = lines.join("\n");

        const table = document.createElement("table");
        table.style.cssText = "width:100%; border-collapse:collapse; margin-top:8px; font-family: ui-monospace, Menlo, Consolas, monospace;";
        table.innerHTML = `
      <thead>
        <tr>
          <th style="text-align:left; padding:6px; border-bottom:1px solid #333;">Ticker</th>
          <th style="text-align:right; padding:6px; border-bottom:1px solid #333;">Weight</th>
          <th style="text-align:right; padding:6px; border-bottom:1px solid #333;">Beta</th>
          <th style="text-align:right; padding:6px; border-bottom:1px solid #333;">w×β</th>
          <th style="text-align:right; padding:6px; border-bottom:1px solid #333;">N</th>
        </tr>
      </thead>
      <tbody></tbody>
      <tfoot>
        <tr>
          <td style="padding:6px; border-top:1px solid #333; font-weight:700;">Portfolio</td>
          <td style="padding:6px; border-top:1px solid #333;"></td>
          <td style="padding:6px; border-top:1px solid #333; text-align:right; font-weight:700;">${Number.isFinite(portfolioBeta) ? portfolioBeta.toFixed(3) : "NaN"}</td>
          <td style="padding:6px; border-top:1px solid #333;"></td>
          <td style="padding:6px; border-top:1px solid #333;"></td>
        </tr>
      </tfoot>
    `;

        const tbody = table.querySelector("tbody");
        for (const r of results.sort((a, b) => b.weight - a.weight)) {
            const wb = (Number.isFinite(r.beta) ? r.weight * r.beta : NaN);
            const tr = document.createElement("tr");
            tr.innerHTML = `
        <td style="padding:6px; border-bottom:1px solid #222;">${escapeHtml(r.ticker)}</td>
        <td style="padding:6px; border-bottom:1px solid #222; text-align:right;">${(r.weight * 100).toFixed(2)}%</td>
        <td style="padding:6px; border-bottom:1px solid #222; text-align:right;">${Number.isFinite(r.beta) ? r.beta.toFixed(3) : "—"}</td>
        <td style="padding:6px; border-bottom:1px solid #222; text-align:right;">${Number.isFinite(wb) ? wb.toFixed(3) : "—"}</td>
        <td style="padding:6px; border-bottom:1px solid #222; text-align:right;">${r.n}</td>
      `;
          if (!Number.isFinite(r.beta) || r.n < 60) tr.style.opacity = "0.65";
          tbody.appendChild(tr);
      }

        $("#mb_tablewrap").innerHTML = "";
        $("#mb_tablewrap").appendChild(table);

        function escapeHtml(s) {
            return String(s)
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#039;");
        }
    }
})();
