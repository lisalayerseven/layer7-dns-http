import { ParquetSchema, ParquetWriter } from "@william.chau/parquetjs-lite";
import parquet from "@william.chau/parquetjs-lite";
import axios from "axios";
import * as cheerio from "cheerio";
import { URL } from "url";
import dns from "dns";
import pLimit from "p-limit";
import parquet from "parquetjs-lite";
import os from "os";

// --- Config ---
const INPUT_FILE = "chunks/test_10000.parquet";
const FINAL_OUT  = "dns_http_text.parquet";

const LOG_INTERVAL = 100;
const TEXT_MIN_CHARS = 200;
const TEXT_MAX_CHARS = 10000;
const TIMEOUT = 8000;
const DNS_CONCURRENCY = 100;
const HTTP_CONCURRENCY = 20;
const TEXT_CONCURRENCY = 10;

// --- Init Resolver (Unbound daemon) ---
const resolver = new dns.Resolver();
resolver.setServers(["127.0.0.1"]);   // Unbound listening on localhost

// --- Schema ---
const schema = new ParquetSchema({
  domain:        { type: "UTF8" },
  has_dns:       { type: "BOOLEAN" },
  dns_ips:       { type: "UTF8", optional: true },
  http_ok:       { type: "BOOLEAN" },
  final_url:     { type: "UTF8", optional: true },
  status_code:   { type: "INT64", optional: true },
  used_https:    { type: "BOOLEAN" },
  text_ok:       { type: "BOOLEAN" },
  homepage_text: { type: "UTF8", optional: true },
  stage:         { type: "UTF8" }  // NEW: pipeline stage reached
});

// --- Metrics helpers ---
function now() { return new Date().getTime(); }
function rate(count, elapsedMs) {
  return (count / (elapsedMs / 1000)).toFixed(2);
}
function memUsage() {
  const m = process.memoryUsage();
  return `RSS ${(m.rss/1e6).toFixed(1)}MB | Heap ${(m.heapUsed/1e6).toFixed(1)}MB / ${(m.heapTotal/1e6).toFixed(1)}MB`;
}
function sysLoad() {
  const loads = os.loadavg().map(v => v.toFixed(2)).join(", ");
  return `Load [1,5,15m]: ${loads}`;
}
function activeHandles() {
  return `Handles: ${process._getActiveHandles().length}`;
}

// --- DNS lookup via Unbound ---
async function dnsLookup(domain) {
  return new Promise((resolve) => {
    resolver.resolve4(domain, (err, addresses) => {
      if (err) return resolve({ has_dns: false, dns_ips: null });
      resolve({ has_dns: true, dns_ips: addresses.join(";") });
    });
  });
}

// --- HTTP helpers ---
function isIpHost(url) {
  try {
    const host = new URL(url).hostname;
    return /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
  } catch {
    return false;
  }
}

async function httpCheck(domain) {
  const urls = [
    `https://${domain}/`,
    `https://www.${domain}/`,
    `http://${domain}/`,
    `http://www.${domain}/`,
  ];

  for (let url of urls) {
    try {
      const resp = await axios.get(url, {
        timeout: TIMEOUT,
        maxRedirects: 5,
        headers: { "User-Agent": "Mozilla/5.0" },
        validateStatus: () => true,
      });
      if (resp.status >= 200 && resp.status < 400) {
        const finalUrl = resp.request.res.responseUrl;
        if (isIpHost(finalUrl)) {
          return { http_ok: false, final_url: null, status_code: null, used_https: false };
        }
        return {
          http_ok: true,
          final_url: finalUrl,
          status_code: resp.status,
          used_https: finalUrl.startsWith("https://"),
        };
      }
    } catch {
      continue;
    }
  }
  return { http_ok: false, final_url: null, status_code: null, used_https: false };
}

async function fetchText(url) {
  try {
    const resp = await axios.get(url, {
      timeout: TIMEOUT,
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    if (resp.status >= 200 && resp.status < 400) {
      const $ = cheerio.load(resp.data);
      $("script, style, noscript").remove();
      let text = $("body").text();
      text = text.replace(/\s+/g, " ").trim();
      if (text.length >= TEXT_MIN_CHARS) {
        return { homepage_text: text.slice(0, TEXT_MAX_CHARS), text_ok: true };
      }
    }
  } catch {}
  return { homepage_text: null, text_ok: false };
}

// --- Utility: write final parquet ---
async function writeParquetFile(filename, rows) {
  const writer = await ParquetWriter.openFile(schema, filename);
  for (const row of rows) {
    await writer.appendRow(row);
  }
  await writer.close();
}

// --- Main pipeline ---
async function main() {
  console.log("Loading input parquet...");
  const reader = await parquet.ParquetReader.openFile(INPUT_FILE);
  const cursor = reader.getCursor();
  const rows = [];
  let rec;
  while ((rec = await cursor.next())) {
    if (rec.domain) rows.push(rec);
  }
  await reader.close();
  console.log(`Loaded ${rows.length} domains`);

  const dnsLimit = pLimit(DNS_CONCURRENCY);
  const httpLimit = pLimit(HTTP_CONCURRENCY);
  const textLimit = pLimit(TEXT_CONCURRENCY);

  const results = [];
  const t0 = now();

  let dnsCount = 0, httpCount = 0, textCount = 0;
  let dnsErrors = 0, httpErrors = 0, textErrors = 0;

  for (let i = 0; i < rows.length; i++) {
    const domain = rows[i].domain;
    let stage = "fail"; // default if nothing works

    try {
      // DNS
      const { has_dns, dns_ips } = await dnsLimit(() => dnsLookup(domain));
      if (!has_dns) { dnsErrors++; results.push({ domain, has_dns, dns_ips, http_ok: false, final_url: null, status_code: null, used_https: false, text_ok: false, homepage_text: null, stage }); continue; }
      dnsCount++;
      stage = "dns";

      // HTTP
      const httpRes = await httpLimit(() => httpCheck(domain));
      if (!httpRes.http_ok) { httpErrors++; results.push({ domain, has_dns, dns_ips, ...httpRes, text_ok: false, homepage_text: null, stage }); continue; }
      httpCount++;
      stage = "http";

      // TEXT
      const textRes = await textLimit(() => fetchText(httpRes.final_url));
      if (!textRes.text_ok) { textErrors++; results.push({ domain, has_dns, dns_ips, ...httpRes, ...textRes, stage }); continue; }
      textCount++;
      stage = "text";

      // Success
      results.push({
        domain,
        has_dns,
        dns_ips,
        ...httpRes,
        ...textRes,
        stage
      });

    } catch (e) {
      dnsErrors++;
      results.push({ domain, has_dns: false, dns_ips: null, http_ok: false, final_url: null, status_code: null, used_https: false, text_ok: false, homepage_text: null, stage: "fail" });
    }

    if ((i + 1) % LOG_INTERVAL === 0 || i + 1 === rows.length) {
      const elapsed = now() - t0;
      console.log(`\n[Progress] ${i + 1}/${rows.length} | Elapsed ${(elapsed/1000).toFixed(1)}s`);
      console.log(`  DNS:  ok=${dnsCount} fail=${dnsErrors} rate=${rate(dnsCount, elapsed)}/s`);
      console.log(`  HTTP: ok=${httpCount} fail=${httpErrors} rate=${rate(httpCount, elapsed)}/s`);
      console.log(`  TEXT: ok=${textCount} fail=${textErrors} rate=${rate(textCount, elapsed)}/s`);
      console.log(`  ${memUsage()} | ${sysLoad()} | ${activeHandles()}`);
    }
  }

  await writeParquetFile(FINAL_OUT, results);

  const totalElapsed = now() - t0;
  console.log("\n📊 Funnel Summary");
  console.log(`Input domains       : ${rows.length}`);
  console.log(`DNS-active          : ${dnsCount} (fail ${dnsErrors})`);
  console.log(`HTTP-responding     : ${httpCount} (fail ${httpErrors})`);
  console.log(`Text usable         : ${textCount} (fail ${textErrors})`);
  console.log(`Final records       : ${results.length}`);
  console.log(`Elapsed time        : ${(totalElapsed/1000).toFixed(1)}s`);
  console.log(`Throughput          : ${rate(rows.length, totalElapsed)} domains/sec`);
  console.log(`System              : ${memUsage()} | ${sysLoad()} | ${activeHandles()}`);
  console.log(`Saved to            : ${FINAL_OUT}`);
}

main().catch(console.error);
