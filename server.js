require("dotenv").config();

const express = require("express");
const compression = require("compression");
const multer = require("multer");
const ExcelJS = require("exceljs");
const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const crypto = require("crypto");
const archiver = require("archiver");
const readline = require("readline");
const os = require("os");

const app = express();
const port = process.env.PORT || 3000;
const isVercelRuntime = String(process.env.VERCEL || "").toLowerCase() === "1";

const projectRoot = __dirname;
const runtimeRoot = isVercelRuntime ? path.join(os.tmpdir(), "roster-generator") : projectRoot;
const excelRoot = path.join(runtimeRoot, "excel");
const uploadsDir = path.join(excelRoot, "uploads");
const jsonDir = path.join(excelRoot, "json");
const jobs = new Map();
const municipalityCacheBuilds = new Map();
const loginAttempts = new Map();
const idLengthHints = new Map([
  ["HHID", 13],
  ["HH_ID", 13],
  ["HHID_ID", 13],
  ["ENTRY_ID", 9]
]);

fs.mkdirSync(uploadsDir, { recursive: true });
fs.mkdirSync(jsonDir, { recursive: true });
app.disable("x-powered-by");
app.set("trust proxy", 1);

const ADMIN_USERNAME = String(process.env.ADMIN_USERNAME || "").trim();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "");
const ADMIN_SESSION_SECRET = String(process.env.ADMIN_SESSION_SECRET || "");
const ADMIN_SESSION_TTL_MS = Number(process.env.ADMIN_SESSION_TTL_MS || 8 * 60 * 60 * 1000);
const ADMIN_COOKIE_NAME = "admin_session";
const adminAuthEnabled = Boolean(ADMIN_USERNAME && ADMIN_PASSWORD && ADMIN_SESSION_SECRET);
const LOGIN_RATE_WINDOW_MS = Number(process.env.LOGIN_RATE_WINDOW_MS || 15 * 60 * 1000);
const LOGIN_MAX_ATTEMPTS = Number(process.env.LOGIN_MAX_ATTEMPTS || 10);
const EXCEL_SHARED_STRINGS_MODE = String(process.env.EXCEL_SHARED_STRINGS_MODE || "cache").toLowerCase();
const EXCEL_STYLES_MODE = String(process.env.EXCEL_STYLES_MODE || "cache").toLowerCase();
const JSON_LINK_SIGNING_SECRET = String(process.env.JSON_LINK_SIGNING_SECRET || ADMIN_SESSION_SECRET).trim();
const JSON_LINK_URL_TTL_SECONDS = Number(process.env.JSON_LINK_URL_TTL_SECONDS || 3600);
const NON_EXPIRING_JSON_FOLDERS = new Set(
  String(process.env.NON_EXPIRING_JSON_FOLDERS || "generate_1")
    .split(",")
    .map((v) => String(v || "").trim())
    .filter(Boolean)
);
const UPSTASH_REDIS_REST_URL = String(process.env.UPSTASH_REDIS_REST_URL || "").trim().replace(/\/+$/g, "");
const UPSTASH_REDIS_REST_TOKEN = String(process.env.UPSTASH_REDIS_REST_TOKEN || "").trim();
const USE_REDIS_JOBS = Boolean(UPSTASH_REDIS_REST_URL && UPSTASH_REDIS_REST_TOKEN);
const JOB_TTL_SECONDS = Number(process.env.JOB_TTL_SECONDS || 24 * 60 * 60);

const allowedMimes = new Set([
  "application/vnd.ms-excel",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "application/octet-stream"
]);

function isExcelFile(file) {
  const ext = path.extname(file.originalname).toLowerCase();
  const validExtension = ext === ".xlsx" || ext === ".xls";
  if (!validExtension) return false;
  if (!file.mimetype) return true;
  return allowedMimes.has(file.mimetype);
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadsDir),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    const base = path.basename(file.originalname, ext).replace(/[^a-zA-Z0-9-_]/g, "_");
    cb(null, `${base}_${Date.now()}${ext}`);
  }
});

const upload = multer({
  storage,
  fileFilter: (_req, file, cb) => {
    if (!isExcelFile(file)) {
      cb(new Error("Invalid file type. Only .xlsx or .xls files are allowed."));
      return;
    }
    cb(null, true);
  },
  limits: { fileSize: 1024 * 1024 * 1024 }
});

const folderSourceUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, uploadsDir),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase();
      cb(null, `folder_source_${Date.now()}${ext}`);
    }
  }),
  fileFilter: (_req, file, cb) => {
    if (!isExcelFile(file)) {
      cb(new Error("Invalid file type. Only .xlsx or .xls files are allowed."));
      return;
    }
    cb(null, true);
  },
  limits: { fileSize: 1024 * 1024 * 1024 }
});

function updateJob(jobId, patch) {
  const current = jobs.get(jobId);
  if (!current) return;
  const updated = { ...current, ...patch, updatedAt: Date.now() };
  jobs.set(jobId, updated);
  void setRemoteJob(jobId, updated);
}

function isActiveJobStatus(status) {
  return status === "queued" || status === "processing";
}

function isJobCancelled(jobId) {
  const job = jobs.get(jobId);
  return Boolean(job && job.cancelRequested);
}

const cancelCheckCache = new Map();

async function ensureNotCancelled(jobId) {
  const localCancelled = isJobCancelled(jobId);
  if (localCancelled) {
    const error = new Error("Job cancelled by user.");
    error.code = "JOB_CANCELLED";
    throw error;
  }
  if (!USE_REDIS_JOBS) return;
  const now = Date.now();
  const cache = cancelCheckCache.get(jobId);
  if (cache && now - cache.checkedAt < 1500) {
    if (cache.cancelRequested) {
      const error = new Error("Job cancelled by user.");
      error.code = "JOB_CANCELLED";
      throw error;
    }
    return;
  }
  const remote = await getRemoteJob(jobId);
  const remoteCancelled = Boolean(remote && remote.cancelRequested);
  cancelCheckCache.set(jobId, { checkedAt: now, cancelRequested: remoteCancelled });
  if (remoteCancelled) {
    updateJob(jobId, { cancelRequested: true });
    const error = new Error("Job cancelled by user.");
    error.code = "JOB_CANCELLED";
    throw error;
  }
}

function formatLongDate(date) {
  return new Intl.DateTimeFormat("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC"
  }).format(date);
}

function formatShortDate(date) {
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const year = String(date.getUTCFullYear());
  return `${month}/${day}/${year}`;
}

function parseStringDate(value) {
  if (typeof value !== "string") return null;
  const text = value.trim();
  if (!text) return null;

  let m = text.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    const y = Number(m[3]);
    if (a < 1 || a > 31 || b < 1 || b > 31) return null;
    const month = a > 12 && b <= 12 ? b : a;
    const day = a > 12 && b <= 12 ? a : b;
    const date = new Date(Date.UTC(y, month - 1, day));
    if (date.getUTCFullYear() === y && date.getUTCMonth() === month - 1 && date.getUTCDate() === day) {
      return date;
    }
    return null;
  }

  m = text.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (m) {
    const y = Number(m[1]);
    const month = Number(m[2]);
    const day = Number(m[3]);
    const date = new Date(Date.UTC(y, month - 1, day));
    if (date.getUTCFullYear() === y && date.getUTCMonth() === month - 1 && date.getUTCDate() === day) {
      return date;
    }
  }
  return null;
}

function inferValueType(value, options = {}) {
  const dateStyle = options.dateStyle === "short" ? "short" : "long";
  if (value === null || value === undefined) return "";
  if (typeof value === "number") return value;
  if (typeof value !== "string") return value;

  const trimmed = value.trim();
  if (trimmed === "") return "";
  const parsedDate = parseStringDate(trimmed);
  if (parsedDate) return dateStyle === "short" ? formatShortDate(parsedDate) : formatLongDate(parsedDate);

  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    const digits = trimmed.replace(/^-/, "");
    const hasLeadingZero = /^\d+$/.test(digits) && digits.length > 1 && digits.startsWith("0");
    const isLongInteger = /^\d+$/.test(digits) && digits.length >= 9;
    if (hasLeadingZero || isLongInteger) return trimmed;

    const num = Number(trimmed);
    if (!Number.isNaN(num) && Number.isFinite(num)) return num;
  }
  return trimmed;
}

function normalizeHeaderName(header) {
  return String(header || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function isLikelyIdentifierHeader(header) {
  const normalized = normalizeHeaderName(header);
  return normalized.includes("ID") || normalized.endsWith("NO") || normalized.endsWith("NUMBER");
}

function isBirthdayHeader(header) {
  const normalized = normalizeHeaderName(header);
  return normalized === "BIRTHDAY"
    || normalized === "BIRTHDATE"
    || normalized === "DATEOFBIRTH"
    || normalized === "DOB";
}

function toSafeKey(header, index) {
  const raw = String(header || "").trim();
  return raw || `column_${index + 1}`;
}

function toSheetName(value) {
  const raw = String(value || "").trim().toUpperCase();
  const cleaned = (raw || "UNKNOWN").replace(/[:\\/?*\[\]]/g, " ").replace(/\s+/g, " ").trim();
  return (cleaned || "UNKNOWN").slice(0, 31);
}

function toWorkbookBaseName(value) {
  const raw = String(value || "").trim().toUpperCase();
  const cleaned = (raw || "UNKNOWN")
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, " ")
    .replace(/\s+/g, " ")
    .replace(/[. ]+$/g, "")
    .trim();
  return cleaned || "UNKNOWN";
}

function uniqueSheetName(baseName, usedNames) {
  let candidate = baseName || "UNKNOWN";
  let counter = 1;
  while (usedNames.has(candidate)) {
    const suffix = `_${counter}`;
    const prefix = Math.max(1, 31 - suffix.length);
    candidate = `${(baseName || "UNKNOWN").slice(0, prefix)}${suffix}`;
    counter += 1;
  }
  usedNames.add(candidate);
  return candidate;
}

function uniqueFileName(baseName, usedNames) {
  let candidate = `${baseName}.xlsx`;
  let counter = 1;
  while (usedNames.has(candidate)) {
    candidate = `${baseName}_${counter}.xlsx`;
    counter += 1;
  }
  usedNames.add(candidate);
  return candidate;
}

function getNextGenerateFolderName() {
  const entries = fs.readdirSync(jsonDir, { withFileTypes: true });
  let max = 0;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const match = entry.name.match(/^generate_(\d+)$/i);
    if (!match) continue;
    const n = Number(match[1]);
    if (Number.isFinite(n) && n > max) max = n;
  }
  return `generate_${max + 1}`;
}

function parseCookies(req) {
  const raw = String((req && req.headers && req.headers.cookie) || "");
  const out = {};
  if (!raw) return out;
  const pairs = raw.split(";");
  for (const pair of pairs) {
    const idx = pair.indexOf("=");
    if (idx < 0) continue;
    const key = pair.slice(0, idx).trim();
    const value = pair.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = decodeURIComponent(value);
  }
  return out;
}

function buildCookie(name, value, maxAgeSeconds) {
  const maxAge = Number.isFinite(maxAgeSeconds) && maxAgeSeconds > 0 ? Math.floor(maxAgeSeconds) : 0;
  const secureFlag = String(process.env.NODE_ENV || "").toLowerCase() === "production" ? "; Secure" : "";
  return `${name}=${encodeURIComponent(value)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secureFlag}`;
}

function clearAdminSessionCookie(res) {
  res.setHeader("Set-Cookie", buildCookie(ADMIN_COOKIE_NAME, "", 0));
}

function timingSafeStringEqual(a, b) {
  const ab = Buffer.from(String(a || ""), "utf8");
  const bb = Buffer.from(String(b || ""), "utf8");
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function createAdminSession() {
  const tokenPayload = crypto.randomBytes(24).toString("hex");
  const expiresAt = Date.now() + ADMIN_SESSION_TTL_MS;
  const sig = crypto
    .createHmac("sha256", ADMIN_SESSION_SECRET)
    .update(`${tokenPayload}.${expiresAt}`)
    .digest("hex");
  const token = `${tokenPayload}.${expiresAt}.${sig}`;
  return { token, expiresAt };
}

function getAdminSession(req) {
  if (!adminAuthEnabled) return null;
  const cookies = parseCookies(req);
  const token = String(cookies[ADMIN_COOKIE_NAME] || "");
  if (!token) return null;

  const [payload, expiresAtRaw, signature] = token.split(".");
  if (!payload || !expiresAtRaw || !signature) return null;
  const expiresAt = Number(expiresAtRaw);
  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) return null;
  const expectedSig = crypto
    .createHmac("sha256", ADMIN_SESSION_SECRET)
    .update(`${payload}.${expiresAt}`)
    .digest("hex");
  if (!timingSafeStringEqual(signature, expectedSig)) return null;
  return { token, expiresAt };
}

function generateAccessToken() {
  return crypto.randomBytes(16).toString("hex");
}

function createJsonLinkSignature(folder, token, exp) {
  return crypto
    .createHmac("sha256", JSON_LINK_SIGNING_SECRET)
    .update(`${folder}:${token}:${exp}`)
    .digest("hex");
}

function buildSignedJsonLinkInfo(folderName, token, sampleMunicipality) {
  const nonExpiring = NON_EXPIRING_JSON_FOLDERS.has(folderName);
  const exp = nonExpiring
    ? null
    : Math.floor(Date.now() / 1000) + Math.max(60, JSON_LINK_URL_TTL_SECONDS);
  const sigPayload = nonExpiring ? "never" : exp;
  const sig = createJsonLinkSignature(folderName, token, sigPayload);
  const encodedFolder = encodeURIComponent(folderName);
  const encodedToken = encodeURIComponent(token);
  const base = `/excel/json/${encodedFolder}/${encodedToken}`;
  const template = nonExpiring
    ? `${base}?sheet={MUNICIPALITY}&sig=${sig}`
    : `${base}?sheet={MUNICIPALITY}&exp=${exp}&sig=${sig}`;
  const sampleSheet = String(sampleMunicipality || "");
  const sample = sampleSheet
    ? (nonExpiring
      ? `${base}?sheet=${encodeURIComponent(sampleSheet)}&sig=${sig}`
      : `${base}?sheet=${encodeURIComponent(sampleSheet)}&exp=${exp}&sig=${sig}`)
    : null;
  return {
    jsonEndpointBase: base,
    jsonUrlTemplate: template,
    sampleUrl: sample,
    expiresAt: exp ? exp * 1000 : null
  };
}

function getWorkbookReaderOptions() {
  const sharedStrings = EXCEL_SHARED_STRINGS_MODE === "emit" ? "emit" : "cache";
  const styles = EXCEL_STYLES_MODE === "ignore" ? "ignore" : "cache";
  return {
    entries: "emit",
    worksheets: "emit",
    sharedStrings,
    styles
  };
}

function getRequestIp(req) {
  return String(req.ip || req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown");
}

function registerFailedLoginAttempt(ip) {
  const now = Date.now();
  const row = loginAttempts.get(ip) || { count: 0, firstAt: now };
  if (now - row.firstAt > LOGIN_RATE_WINDOW_MS) {
    loginAttempts.set(ip, { count: 1, firstAt: now });
    return;
  }
  loginAttempts.set(ip, { count: row.count + 1, firstAt: row.firstAt });
}

function clearFailedLoginAttempts(ip) {
  loginAttempts.delete(ip);
}

function isLoginRateLimited(ip) {
  const now = Date.now();
  const row = loginAttempts.get(ip);
  if (!row) return false;
  if (now - row.firstAt > LOGIN_RATE_WINDOW_MS) {
    loginAttempts.delete(ip);
    return false;
  }
  return row.count >= LOGIN_MAX_ATTEMPTS;
}

function getJobStorageKey(jobId) {
  return `job:${jobId}`;
}

async function upstashCommand(command, args = []) {
  if (!USE_REDIS_JOBS) return null;
  const response = await fetch(UPSTASH_REDIS_REST_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_REDIS_REST_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify([command, ...args])
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Upstash command failed (${response.status}): ${text || command}`);
  }
  return response.json();
}

async function setRemoteJob(jobId, job) {
  if (!USE_REDIS_JOBS) return;
  try {
    const key = getJobStorageKey(jobId);
    const payload = JSON.stringify(job);
    await upstashCommand("SET", [key, payload, "EX", String(JOB_TTL_SECONDS)]);
  } catch (_e) {
    // noop - keep local fallback
  }
}

async function getRemoteJob(jobId) {
  if (!USE_REDIS_JOBS) return null;
  try {
    const key = getJobStorageKey(jobId);
    const result = await upstashCommand("GET", [key]);
    const raw = result && Object.prototype.hasOwnProperty.call(result, "result")
      ? result.result
      : null;
    if (!raw || typeof raw !== "string") return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_e) {
    return null;
  }
}

async function getJob(jobId) {
  const local = jobs.get(jobId);
  if (!USE_REDIS_JOBS) return local || null;
  const remote = await getRemoteJob(jobId);
  if (remote) {
    jobs.set(jobId, remote);
    return remote;
  }
  return local || null;
}

function requireAdminAuth(req, res, next) {
  if (!adminAuthEnabled) {
    return res.status(503).json({
      error: "Admin auth is disabled. Set ADMIN_USERNAME, ADMIN_PASSWORD, and ADMIN_SESSION_SECRET."
    });
  }
  const session = getAdminSession(req);
  if (!session) return res.status(401).json({ error: "Unauthorized." });
  req.adminSession = session;
  return next();
}

function getDirectoryTreeStats(dirPath) {
  let bytes = 0;
  let fileCount = 0;
  let dirCount = 0;
  const stack = [dirPath];
  while (stack.length) {
    const current = stack.pop();
    let entries = [];
    try { entries = fs.readdirSync(current, { withFileTypes: true }); } catch (_e) { continue; }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        dirCount += 1;
        stack.push(full);
        continue;
      }
      if (entry.isFile()) {
        fileCount += 1;
        try {
          const stat = fs.statSync(full);
          bytes += Number(stat.size || 0);
        } catch (_e) {
          // noop
        }
      }
    }
  }
  return { bytes, fileCount, dirCount };
}

function listGenerateFolders() {
  let entries = [];
  try { entries = fs.readdirSync(jsonDir, { withFileTypes: true }); } catch (_e) { return []; }
  const out = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    if (!/^generate_\d+$/i.test(entry.name)) continue;
    const dirPath = path.join(jsonDir, entry.name);
    let stat = null;
    try { stat = fs.statSync(dirPath); } catch (_e) { stat = null; }
    const stats = getDirectoryTreeStats(dirPath);
    out.push({
      name: entry.name,
      path: dirPath,
      modifiedAt: stat ? stat.mtimeMs : 0,
      fileCount: stats.fileCount,
      dirCount: stats.dirCount,
      sizeBytes: stats.bytes
    });
  }
  return out.sort((a, b) => b.modifiedAt - a.modifiedAt);
}

function getCellPrimitive(cell) {
  const value = cell ? cell.value : null;
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "object") {
    if (Object.prototype.hasOwnProperty.call(value, "result")) return value.result ?? "";
    if (Object.prototype.hasOwnProperty.call(value, "text")) return String(value.text || "");
    if (Array.isArray(value.richText)) return value.richText.map((p) => p.text || "").join("");
  }
  return String(value);
}

function convertCell(cell, isIdentifier, dateStyle = "long") {
  const text = cell && typeof cell.text === "string" ? cell.text.trim() : "";
  const raw = getCellPrimitive(cell);
  if (isIdentifier) {
    if (text) return text;
    if (raw === null || raw === undefined || raw === "") return "";
    return String(raw).trim();
  }
  if (raw instanceof Date) {
    const utc = new Date(Date.UTC(raw.getUTCFullYear(), raw.getUTCMonth(), raw.getUTCDate()));
    return dateStyle === "short" ? formatShortDate(utc) : formatLongDate(utc);
  }
  if (typeof raw === "string") return inferValueType(raw, { dateStyle });
  if (typeof raw === "number") return raw;
  if (typeof raw === "boolean") return raw;
  return raw ?? "";
}

function expandScientificInteger(text) {
  const raw = String(text || "").trim();
  if (!/^-?\d+(\.\d+)?[eE][+-]?\d+$/.test(raw)) return raw;
  const negative = raw.startsWith("-");
  const unsigned = negative ? raw.slice(1) : raw;
  const [mantissa, expText] = unsigned.toLowerCase().split("e");
  const exp = Number(expText);
  if (!Number.isFinite(exp)) return raw;
  const parts = mantissa.split(".");
  const whole = parts[0] || "0";
  const frac = parts[1] || "";
  const digits = `${whole}${frac}`.replace(/^0+(?=\d)/, "");
  const decimalPos = whole.length + exp;
  if (decimalPos < 0) return raw;
  if (decimalPos >= digits.length) {
    return `${negative ? "-" : ""}${digits}${"0".repeat(decimalPos - digits.length)}`;
  }
  const intPart = digits.slice(0, decimalPos);
  const fracPart = digits.slice(decimalPos).replace(/0+$/, "");
  if (!fracPart) return `${negative ? "-" : ""}${intPart || "0"}`;
  return raw;
}

function normalizeIdentifierValue(header, value) {
  let text = String(value ?? "").trim();
  if (!text) return "";
  text = expandScientificInteger(text);
  if (/^\d+\.0+$/.test(text)) text = text.replace(/\.0+$/, "");
  const target = idLengthHints.get(normalizeHeaderName(header)) || 0;
  if (target > 0 && /^\d+$/.test(text) && text.length < target) {
    text = text.padStart(target, "0");
  }
  return text;
}

function buildNameProjection(sourceHeaders) {
  const firstIdx = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "FIRSTNAME");
  const middleIdx = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "MIDDLENAME");
  const lastIdx = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "LASTNAME");
  const extIdx = sourceHeaders.findIndex((h) => {
    const n = normalizeHeaderName(h);
    return n === "EXTNAME" || n === "EXT";
  });

  if (firstIdx < 0 || middleIdx < 0 || lastIdx < 0) {
    return null;
  }

  const dropSet = new Set([firstIdx, middleIdx, lastIdx]);
  if (extIdx >= 0) dropSet.add(extIdx);
  const insertAt = Math.min(firstIdx, middleIdx, lastIdx, extIdx >= 0 ? extIdx : Number.MAX_SAFE_INTEGER);

  const outputHeaders = [];
  const outputToSource = [];
  let namesOutputIndex = -1;

  for (let i = 0; i < sourceHeaders.length; i += 1) {
    if (i === insertAt) {
      namesOutputIndex = outputHeaders.length;
      outputHeaders.push("NAMES");
      outputToSource.push(-1);
    }
    if (dropSet.has(i)) continue;
    outputHeaders.push(sourceHeaders[i]);
    outputToSource.push(i);
  }

  if (namesOutputIndex < 0) {
    namesOutputIndex = outputHeaders.length;
    outputHeaders.push("NAMES");
    outputToSource.push(-1);
  }

  return {
    firstIdx,
    middleIdx,
    lastIdx,
    extIdx,
    namesOutputIndex,
    outputHeaders,
    outputToSource
  };
}

function joinNameParts(first, middle, last, ext) {
  const firstText = String(first || "").trim();
  const middleText = String(middle || "").trim();
  const lastText = String(last || "").trim();
  const extText = String(ext || "").trim();

  const tail = [middleText, lastText, extText].filter(Boolean).join(" ").trim();
  if (firstText && tail) return `${firstText}, ${tail}`;
  if (firstText) return firstText;
  return tail;
}

function buildOutputProjection(sourceHeaders) {
  const nameProjection = buildNameProjection(sourceHeaders);
  const outputHeaders = nameProjection
    ? nameProjection.outputHeaders
    : sourceHeaders.slice();
  const outputToSource = nameProjection
    ? nameProjection.outputToSource
    : sourceHeaders.map((_h, idx) => idx);
  const outputIdColumnFlags = outputHeaders.map((h) => isLikelyIdentifierHeader(h));
  const outputBirthdayColumnFlags = outputHeaders.map((h) => isBirthdayHeader(h));

  return {
    nameProjection,
    outputHeaders,
    outputToSource,
    outputIdColumnFlags,
    outputBirthdayColumnFlags
  };
}

function buildProjectedRow(row, projection) {
  const outValues = new Array(projection.outputHeaders.length);
  for (let outCol = 0; outCol < projection.outputHeaders.length; outCol += 1) {
    if (projection.nameProjection && outCol === projection.nameProjection.namesOutputIndex) {
      const first = convertCell(row.getCell(projection.nameProjection.firstIdx + 1), false);
      const middle = convertCell(row.getCell(projection.nameProjection.middleIdx + 1), false);
      const last = convertCell(row.getCell(projection.nameProjection.lastIdx + 1), false);
      const ext = projection.nameProjection.extIdx >= 0
        ? convertCell(row.getCell(projection.nameProjection.extIdx + 1), false)
        : "";
      outValues[outCol] = joinNameParts(first, middle, last, ext);
      continue;
    }

    const srcIdx = projection.outputToSource[outCol];
    const header = projection.outputHeaders[outCol];
    const isId = projection.outputIdColumnFlags[outCol];
    const dateStyle = "long";
    const cell = row.getCell(srcIdx + 1);
    const converted = convertCell(cell, isId, dateStyle);
    outValues[outCol] = isId ? normalizeIdentifierValue(header, converted) : converted;
  }
  return outValues;
}

function normalizeMunicipality(value) {
  return String(value || "").trim();
}

function toSafeFilePart(value) {
  return String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9-_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80) || "UNKNOWN";
}

function getMunicipalityCacheKey(sheet) {
  return normalizeMunicipality(sheet).toUpperCase();
}

function getMunicipalityCacheFileName(sheetKey) {
  const safe = toSafeFilePart(sheetKey);
  const hash = crypto.createHash("sha1").update(sheetKey).digest("hex").slice(0, 12);
  return `${safe}_${hash}.jsonl`;
}

async function writeChunk(res, chunk) {
  if (res.write(chunk)) return;
  await new Promise((resolve) => res.once("drain", resolve));
}

async function streamJsonRowsArrayFromJsonl(res, jsonlPath) {
  let rowCount = 0;
  let isFirst = true;
  const rl = readline.createInterface({
    input: fs.createReadStream(jsonlPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    if (!line) continue;
    if (!isFirst) await writeChunk(res, ",");
    await writeChunk(res, line);
    isFirst = false;
    rowCount += 1;
  }
  return rowCount;
}

async function buildMunicipalityJsonCache(batchDir, metadata, requestedSheet) {
  const sourcePath = path.join(batchDir, metadata.sourceFileName || "source.xlsx");
  if (!fs.existsSync(sourcePath)) {
    throw new Error("Source file expired or missing.");
  }

  const requestedKey = getMunicipalityCacheKey(requestedSheet);
  const cacheDir = path.join(batchDir, "cache");
  fs.mkdirSync(cacheDir, { recursive: true });

  const cacheFileName = getMunicipalityCacheFileName(requestedKey);
  const cachePath = path.join(cacheDir, cacheFileName);
  const tempPath = `${cachePath}.tmp`;

  const reader = new ExcelJS.stream.xlsx.WorkbookReader(sourcePath, getWorkbookReaderOptions());

  const out = fs.createWriteStream(tempPath, { encoding: "utf8" });
  let foundSheet = false;
  let sourceHeaders = [];
  let municipalityIndex = -1;
  let projection = null;
  let rowCount = 0;

  try {
    for await (const worksheetReader of reader) {
      if (foundSheet) break;
      foundSheet = true;
      for await (const row of worksheetReader) {
        if (row.number === 1) {
          const values = row.values || [];
          const maxIndex = values.length - 1;
          sourceHeaders = [];
          for (let col = 1; col <= maxIndex; col += 1) {
            sourceHeaders.push(toSafeKey(values[col], col - 1));
          }
          projection = buildOutputProjection(sourceHeaders);
          municipalityIndex = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "MUNICIPALITY");
          if (municipalityIndex < 0) {
            throw new Error("MUNICIPALITY column not found.");
          }
          continue;
        }

        if (!projection) continue;
        const municipalityRaw = convertCell(row.getCell(municipalityIndex + 1), false);
        const municipalityKey = getMunicipalityCacheKey(municipalityRaw) || "UNKNOWN";
        if (municipalityKey !== requestedKey) continue;

        const outValues = buildProjectedRow(row, projection);
        const rowObject = {};
        for (let i = 0; i < projection.outputHeaders.length; i += 1) {
          rowObject[projection.outputHeaders[i]] = outValues[i];
        }

        if (!out.write(`${JSON.stringify(rowObject)}\n`)) {
          await new Promise((resolve) => out.once("drain", resolve));
        }
        rowCount += 1;
      }
    }

    if (!foundSheet) {
      throw new Error("Workbook has no sheets.");
    }

    await new Promise((resolve, reject) => {
      out.end(() => resolve());
      out.on("error", reject);
    });
    fs.renameSync(tempPath, cachePath);
  } catch (error) {
    try { out.destroy(); } catch (_e) { /* noop */ }
    try { if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath); } catch (_e) { /* noop */ }
    throw error;
  }

  const metadataPath = path.join(batchDir, "metadata.json");
  const latestMeta = JSON.parse(fs.readFileSync(metadataPath, "utf8"));
  const municipalityCache = latestMeta.municipalityCache && typeof latestMeta.municipalityCache === "object"
    ? latestMeta.municipalityCache
    : {};
  municipalityCache[requestedKey] = {
    file: cacheFileName,
    rowCount,
    createdAt: Date.now()
  };
  latestMeta.municipalityCache = municipalityCache;
  fs.writeFileSync(metadataPath, JSON.stringify(latestMeta));

  return { file: cacheFileName, rowCount };
}

async function ensureMunicipalityJsonCache(batchDir, metadata, requestedSheet) {
  const requestedKey = getMunicipalityCacheKey(requestedSheet);
  const knownCache = metadata.municipalityCache && metadata.municipalityCache[requestedKey];
  if (knownCache && knownCache.file) {
    const knownPath = path.join(batchDir, "cache", knownCache.file);
    if (fs.existsSync(knownPath)) {
      return { cachePath: knownPath, rowCount: Number(knownCache.rowCount) || 0 };
    }
  }

  const buildKey = `${batchDir}::${requestedKey}`;
  if (municipalityCacheBuilds.has(buildKey)) {
    const shared = await municipalityCacheBuilds.get(buildKey);
    return {
      cachePath: path.join(batchDir, "cache", shared.file),
      rowCount: Number(shared.rowCount) || 0
    };
  }

  const buildPromise = buildMunicipalityJsonCache(batchDir, metadata, requestedSheet);
  municipalityCacheBuilds.set(buildKey, buildPromise);
  try {
    const built = await buildPromise;
    return {
      cachePath: path.join(batchDir, "cache", built.file),
      rowCount: Number(built.rowCount) || 0
    };
  } finally {
    municipalityCacheBuilds.delete(buildKey);
  }
}

async function createZipArchive(batchDir, zipFilePath, fileNames) {
  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", resolve);
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    for (const fileName of fileNames) {
      archive.file(path.join(batchDir, fileName), { name: fileName });
    }
    archive.finalize();
  });
}

function normalizeSplitCount(rawValue) {
  const n = Number(rawValue);
  if (!Number.isInteger(n)) return 2;
  if (n < 2) return 2;
  if (n > 100) return 100;
  return n;
}

function buildMunicipalitySplitPlanFromList(municipalities, splitCount) {
  const total = municipalities.length;
  const parts = normalizeSplitCount(splitCount);
  const baseSize = Math.floor(total / parts);
  const remainder = total % parts;
  const municipalityToPart = new Map();
  const municipalitiesPerWorkbook = new Array(parts).fill(0);

  let idx = 0;
  for (let part = 0; part < parts; part += 1) {
    const size = baseSize + (part < remainder ? 1 : 0);
    municipalitiesPerWorkbook[part] = size;
    for (let i = 0; i < size; i += 1) {
      municipalityToPart.set(municipalities[idx], part);
      idx += 1;
    }
  }

  return {
    splitCount: parts,
    municipalityTotal: total,
    municipalitiesPerWorkbook,
    municipalityToPart
  };
}

async function buildMunicipalitySplitPlan(filePath, jobId, splitCount) {
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, getWorkbookReaderOptions());

  let foundSheet = false;
  let headers = [];
  let municipalityIndex = -1;
  let splitBy = null;
  let scannedRows = 0;
  const seen = new Set();
  const municipalities = [];

  for await (const worksheetReader of reader) {
    if (foundSheet) break;
    foundSheet = true;
    for await (const row of worksheetReader) {
      await ensureNotCancelled(jobId);
      const values = row.values || [];
      if (row.number === 1) {
        const maxIndex = values.length - 1;
        headers = [];
        for (let col = 1; col <= maxIndex; col += 1) {
          headers.push(toSafeKey(values[col], col - 1));
        }
        municipalityIndex = headers.findIndex((h) => normalizeHeaderName(h) === "MUNICIPALITY");
        splitBy = municipalityIndex >= 0 ? headers[municipalityIndex] : null;
        continue;
      }

      if (municipalityIndex < 0) continue;
      const municipalityRaw = convertCell(row.getCell(municipalityIndex + 1), false);
      const municipalityKey = String(municipalityRaw || "").trim() || "UNKNOWN";
      if (!seen.has(municipalityKey)) {
        seen.add(municipalityKey);
        municipalities.push(municipalityKey);
      }

      scannedRows += 1;
      if (scannedRows % 10000 === 0) {
        updateJob(jobId, {
          progress: 18,
          message: `Planning split... (${municipalities.length.toLocaleString()} municipalities found)`
        });
      }
    }
  }

  if (!foundSheet) {
    throw new Error("Workbook has no sheets.");
  }

  const plan = buildMunicipalitySplitPlanFromList(municipalities, splitCount);
  return {
    municipalityIndex,
    splitBy,
    ...plan
  };
}

function clearFolderCacheFiles(batchDir) {
  const cacheDir = path.join(batchDir, "cache");
  if (!fs.existsSync(cacheDir)) return;
  try { fs.rmSync(cacheDir, { recursive: true, force: true }); } catch (_e) { /* noop */ }
}

function loadFolderMetadata(batchDir) {
  const metadataPath = path.join(batchDir, "metadata.json");
  if (!fs.existsSync(metadataPath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(metadataPath, "utf8"));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_e) {
    return null;
  }
}

async function loadFolderMetadataAsync(batchDir) {
  const metadataPath = path.join(batchDir, "metadata.json");
  try {
    const raw = await fsp.readFile(metadataPath, "utf8");
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_e) {
    return null;
  }
}

async function clearFolderCacheFilesAsync(batchDir) {
  const cacheDir = path.join(batchDir, "cache");
  try {
    await fsp.rm(cacheDir, { recursive: true, force: true });
  } catch (_e) {
    // noop
  }
}

async function scanWorkbookForJsonMetadata(sourcePath, jobId = null) {
  const municipalityRowCounts = new Map();
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(sourcePath, getWorkbookReaderOptions());

  let processedRows = 0;
  let foundSheet = false;
  let sourceHeaders = [];
  let municipalityIndex = -1;
  let splitBy = null;

  for await (const worksheetReader of reader) {
    if (foundSheet) break;
    foundSheet = true;
    for await (const row of worksheetReader) {
      if (jobId) await ensureNotCancelled(jobId);
      if (row.number === 1) {
        const values = row.values || [];
        const maxIndex = values.length - 1;
        sourceHeaders = [];
        for (let col = 1; col <= maxIndex; col += 1) {
          sourceHeaders.push(toSafeKey(values[col], col - 1));
        }
        municipalityIndex = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "MUNICIPALITY");
        splitBy = municipalityIndex >= 0 ? sourceHeaders[municipalityIndex] : null;
        continue;
      }

      const municipalityRaw = municipalityIndex >= 0
        ? convertCell(row.getCell(municipalityIndex + 1), false)
        : "";
      const municipalityKey = normalizeMunicipality(municipalityRaw) || "UNKNOWN";
      municipalityRowCounts.set(municipalityKey, (municipalityRowCounts.get(municipalityKey) || 0) + 1);
      processedRows += 1;

      if (jobId && processedRows % 10000 === 0) {
        updateJob(jobId, {
          progress: Math.min(92, 15 + Math.floor(processedRows / 15000)),
          message: `Scanning rows... ${processedRows.toLocaleString()}`
        });
      }
    }
  }

  if (!foundSheet) throw new Error("Workbook has no sheets.");
  if (municipalityIndex < 0) throw new Error("MUNICIPALITY column not found. JSON link mode requires MUNICIPALITY.");

  const projection = buildOutputProjection(sourceHeaders);
  return {
    sourceHeaders,
    outputHeaders: projection.outputHeaders,
    municipalityIndex,
    splitBy,
    rowCount: processedRows,
    municipalityStats: Array.from(municipalityRowCounts.entries())
      .map(([municipality, rows]) => ({ municipality, rows }))
      .sort((a, b) => b.rows - a.rows)
  };
}

async function rebuildJsonFolderMetadata(folderName, options = {}) {
  const batchDir = path.join(jsonDir, folderName);
  const sourceFileName = String(options.sourceFileName || "source.xlsx");
  const sourcePath = path.join(batchDir, sourceFileName);
  try { await fsp.access(sourcePath, fs.constants.F_OK); } catch (_e) { throw new Error("Source file not found in target folder."); }

  const current = await loadFolderMetadataAsync(batchDir) || {};
  const token = String(options.token || current.token || generateAccessToken()).trim();
  if (!token) throw new Error("Missing JSON token.");

  const scan = await scanWorkbookForJsonMetadata(sourcePath, options.jobId || null);
  await clearFolderCacheFilesAsync(batchDir);

  let sourceMtimeMs = 0;
  try { sourceMtimeMs = Number((await fsp.stat(sourcePath)).mtimeMs || 0); } catch (_e) { sourceMtimeMs = 0; }

  const metadata = {
    token,
    sourceFileName,
    originalFileName: String(options.originalFileName || current.originalFileName || sourceFileName),
    createdAt: Number(current.createdAt) || Date.now(),
    updatedAt: Date.now(),
    workbookMode: "multiple",
    outputMode: "json",
    splitBy: scan.splitBy || null,
    municipalityColumnIndex: scan.municipalityIndex,
    sourceHeaders: scan.sourceHeaders,
    outputHeaders: scan.outputHeaders,
    municipalityCache: {},
    municipalityStats: scan.municipalityStats,
    rowCount: scan.rowCount,
    sourceMtimeMs
  };

  await fsp.writeFile(path.join(batchDir, "metadata.json"), JSON.stringify(metadata));
  return metadata;
}

async function processUploadJob(jobId, filePath, originalName, workbookModeInput, splitCountInput, outputModeInput) {
  const outputMode = outputModeInput === "json" ? "json" : "zip";
  if (outputMode === "json" && workbookModeInput === "multiple") {
    return processUploadJobForJsonLink(jobId, filePath, originalName);
  }

  const municipalityRowCounts = new Map();
  const workbookBundles = new Map();
  const singleSheetBundles = new Map();
  const singleUsedSheetNames = new Set();
  const splitSheetBundles = [];
  const splitUsedSheetNames = [];

  const workbookMode = ["single", "multiple", "split"].includes(workbookModeInput)
    ? workbookModeInput
    : "single";
  const splitCount = normalizeSplitCount(splitCountInput);

  let batchDir = "";
  let outputPath = "";
  const generatedWorkbookFiles = [];
  let singleWorkbookBundle = null;
  const splitWorkbookBundles = [];
  let splitPlan = null;

  try {
    updateJob(jobId, { status: "processing", progress: 3, message: "Opening file..." });
    await ensureNotCancelled(jobId);
    updateJob(jobId, { progress: 12, message: "Preparing output files..." });

    if (workbookMode === "split") {
      splitPlan = await buildMunicipalitySplitPlan(filePath, jobId, splitCount);
      updateJob(jobId, {
        progress: 20,
        message: `Split plan ready: ${splitPlan.municipalityTotal} municipalities into ${splitPlan.splitCount} workbooks`
      });
    }

    const batchFolderName = getNextGenerateFolderName();
    batchDir = path.join(jsonDir, batchFolderName);
    fs.mkdirSync(batchDir, { recursive: true });
    updateJob(jobId, { outputFolder: batchFolderName });

    const sourceBase = path.basename(originalName, path.extname(originalName)).replace(/[^a-zA-Z0-9-_]+/g, "_");
    const outputFileName = `${sourceBase || "OUTPUT"}_${Date.now()}.zip`;
    outputPath = path.join(batchDir, outputFileName);

    const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, getWorkbookReaderOptions());

    const usedWorkbookFileNames = new Set();
    let sourceHeaders = [];
    let outputHeaders = [];
    let outputIdColumnFlags = [];
    let outputBirthdayColumnFlags = [];
    let outputToSource = [];
    let nameProjection = null;
    let municipalityIndex = -1;
    let splitBy = null;
    let processedRows = 0;
    let foundSheet = false;

    if (workbookMode === "split") {
      const parts = splitPlan ? splitPlan.splitCount : 2;
      for (let part = 0; part < parts; part += 1) {
        const wbFile = uniqueFileName(
          toWorkbookBaseName(`${sourceBase || "OUTPUT"}_PART_${part + 1}`),
          usedWorkbookFileNames
        );
        const wbPath = path.join(batchDir, wbFile);
        splitWorkbookBundles.push({
          workbookFile: wbFile,
          workbookPath: wbPath,
          workbook: new ExcelJS.stream.xlsx.WorkbookWriter({
            filename: wbPath,
            useStyles: false,
            useSharedStrings: false
          })
        });
        splitSheetBundles.push(new Map());
        splitUsedSheetNames.push(new Set());
        generatedWorkbookFiles.push(wbFile);
      }
    }

    for await (const worksheetReader of reader) {
      if (foundSheet) break;
      foundSheet = true;
      for await (const row of worksheetReader) {
        await ensureNotCancelled(jobId);
        const values = row.values || [];

        if (row.number === 1) {
          const maxIndex = values.length - 1;
          sourceHeaders = [];
          for (let col = 1; col <= maxIndex; col += 1) {
            sourceHeaders.push(toSafeKey(values[col], col - 1));
          }

          const projection = buildOutputProjection(sourceHeaders);
          nameProjection = projection.nameProjection;
          outputHeaders = projection.outputHeaders;
          outputToSource = projection.outputToSource;
          outputIdColumnFlags = projection.outputIdColumnFlags;
          outputBirthdayColumnFlags = projection.outputBirthdayColumnFlags;

          municipalityIndex = sourceHeaders.findIndex((h) => normalizeHeaderName(h) === "MUNICIPALITY");
          splitBy = municipalityIndex >= 0 ? sourceHeaders[municipalityIndex] : null;
          continue;
        }

        if (!outputHeaders.length) continue;

        const outValues = new Array(outputHeaders.length);
        for (let outCol = 0; outCol < outputHeaders.length; outCol += 1) {
          if (nameProjection && outCol === nameProjection.namesOutputIndex) {
            const first = convertCell(row.getCell(nameProjection.firstIdx + 1), false);
            const middle = convertCell(row.getCell(nameProjection.middleIdx + 1), false);
            const last = convertCell(row.getCell(nameProjection.lastIdx + 1), false);
            const ext = nameProjection.extIdx >= 0
              ? convertCell(row.getCell(nameProjection.extIdx + 1), false)
              : "";
            outValues[outCol] = joinNameParts(first, middle, last, ext);
            continue;
          }

          const srcIdx = outputToSource[outCol];
          const header = outputHeaders[outCol];
          const isId = outputIdColumnFlags[outCol];
          const dateStyle = "long";
          const cell = row.getCell(srcIdx + 1);
          const converted = convertCell(cell, isId, dateStyle);
          outValues[outCol] = isId ? normalizeIdentifierValue(header, converted) : converted;
        }

        const municipalityRaw = municipalityIndex >= 0
          ? convertCell(row.getCell(municipalityIndex + 1), false)
          : "ALL_DATA";
        const municipalityKey = String(municipalityRaw || "").trim() || "UNKNOWN";
        let bundle;

        if (workbookMode === "single") {
          if (!singleWorkbookBundle) {
            const baseName = toWorkbookBaseName(`${sourceBase || "OUTPUT"}_ALL_MUNICIPALITIES`);
            const workbookFile = uniqueFileName(baseName, usedWorkbookFileNames);
            const workbookPath = path.join(batchDir, workbookFile);
            const wb = new ExcelJS.stream.xlsx.WorkbookWriter({
              filename: workbookPath,
              useStyles: false,
              useSharedStrings: false
            });
            singleWorkbookBundle = { workbook: wb, workbookFile, workbookPath };
            generatedWorkbookFiles.push(workbookFile);
          }

          bundle = singleSheetBundles.get(municipalityKey);
          if (!bundle) {
            const sheetName = uniqueSheetName(toSheetName(municipalityKey), singleUsedSheetNames);
            const ws = singleWorkbookBundle.workbook.addWorksheet(sheetName);
            ws.addRow(outputHeaders).commit();
            bundle = { worksheet: ws };
            singleSheetBundles.set(municipalityKey, bundle);
          }
        } else if (workbookMode === "multiple") {
          bundle = workbookBundles.get(municipalityKey);
          if (!bundle) {
            const baseName = toWorkbookBaseName(municipalityKey);
            const workbookFile = uniqueFileName(baseName, usedWorkbookFileNames);
            const workbookPath = path.join(batchDir, workbookFile);
            const wb = new ExcelJS.stream.xlsx.WorkbookWriter({
              filename: workbookPath,
              useStyles: false,
              useSharedStrings: false
            });
            const sheetName = uniqueSheetName(toSheetName(municipalityKey), new Set());
            const ws = wb.addWorksheet(sheetName);
            ws.addRow(outputHeaders).commit();
            bundle = { workbook: wb, worksheet: ws, workbookFile, workbookPath };
            workbookBundles.set(municipalityKey, bundle);
            generatedWorkbookFiles.push(workbookFile);
          }
        } else {
          const mappedPart = splitPlan && splitPlan.municipalityToPart.has(municipalityKey)
            ? splitPlan.municipalityToPart.get(municipalityKey)
            : 0;
          const safePart = Number.isInteger(mappedPart) && mappedPart >= 0 && mappedPart < splitWorkbookBundles.length
            ? mappedPart
            : 0;
          const targetSheets = splitSheetBundles[safePart];
          const targetUsedNames = splitUsedSheetNames[safePart];
          const targetWorkbook = splitWorkbookBundles[safePart];

          bundle = targetSheets.get(municipalityKey);
          if (!bundle) {
            const sheetName = uniqueSheetName(toSheetName(municipalityKey), targetUsedNames);
            const ws = targetWorkbook.workbook.addWorksheet(sheetName);
            ws.addRow(outputHeaders).commit();
            bundle = { worksheet: ws };
            targetSheets.set(municipalityKey, bundle);
          }
        }

        bundle.worksheet.addRow(outValues).commit();

        const muniRows = (municipalityRowCounts.get(municipalityKey) || 0) + 1;
        municipalityRowCounts.set(municipalityKey, muniRows);
        processedRows += 1;

        if (processedRows % 200 === 0) {
          const progress = Math.min(90, 25 + Math.floor(processedRows / 10000));
          updateJob(jobId, {
            progress,
            message: "Converting rows...",
            currentMunicipality: municipalityKey,
            currentMunicipalityRows: muniRows,
            currentWorkbookPart: workbookMode === "split" && splitPlan
              ? (splitPlan.municipalityToPart.get(municipalityKey) || 0) + 1
              : null,
            processedRows
          });
        }
      }
    }

    if (!foundSheet) throw new Error("Workbook has no sheets.");
    await ensureNotCancelled(jobId);

    if (workbookMode === "single") {
      for (const bundle of singleSheetBundles.values()) {
        bundle.worksheet.commit();
      }
      if (singleWorkbookBundle) {
        await singleWorkbookBundle.workbook.commit();
      }
    } else if (workbookMode === "multiple") {
      for (const bundle of workbookBundles.values()) {
        bundle.worksheet.commit();
        await bundle.workbook.commit();
      }
    } else {
      for (const sheets of splitSheetBundles) {
        for (const bundle of sheets.values()) {
          bundle.worksheet.commit();
        }
      }
      for (const wbBundle of splitWorkbookBundles) {
        await wbBundle.workbook.commit();
      }
    }

    await ensureNotCancelled(jobId);

    updateJob(jobId, { progress: 97, message: "Packaging ZIP..." });
    await createZipArchive(batchDir, outputPath, generatedWorkbookFiles);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);

    const encodedFolder = encodeURIComponent(batchFolderName);
    const encodedFile = encodeURIComponent(outputFileName);
    const result = {
      message: splitBy
        ? (workbookMode === "single"
          ? "ZIP file generated successfully with one workbook and municipality sheets."
          : workbookMode === "split"
            ? "ZIP file generated successfully with split workbooks."
            : "ZIP file generated successfully with municipality workbooks.")
        : "MUNICIPALITY column not found. Generated a single workbook in ZIP.",
      rowCount: processedRows,
      outputMode: "zip",
      workbookMode,
      splitBy: splitBy || null,
      splitWorkbookSummary: workbookMode === "split" && splitPlan
        ? {
          totalMunicipalities: splitPlan.municipalityTotal,
          splitCount: splitPlan.splitCount,
          municipalitiesPerWorkbook: splitPlan.municipalitiesPerWorkbook
        }
        : null,
      outputFolder: batchFolderName,
      fileCount: generatedWorkbookFiles.length || 1,
      outputFileName,
      downloadUrl: `/download/${encodedFolder}/${encodedFile}`,
      municipalityStats: Array.from(municipalityRowCounts.entries())
        .map(([municipality, rows]) => ({ municipality, rows }))
        .sort((a, b) => b.rows - a.rows)
    };

    updateJob(jobId, { status: "completed", progress: 100, message: "Done", result });
  } catch (error) {
    try { if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (_e) { /* noop */ }
    if (singleWorkbookBundle) {
      if (singleWorkbookBundle.workbook && typeof singleWorkbookBundle.workbook.cancel === "function") {
        try { singleWorkbookBundle.workbook.cancel(); } catch (_e) { /* noop */ }
      }
      if (singleWorkbookBundle.workbookPath && fs.existsSync(singleWorkbookBundle.workbookPath)) {
        try { fs.unlinkSync(singleWorkbookBundle.workbookPath); } catch (_e) { /* noop */ }
      }
    }
    for (const bundle of workbookBundles.values()) {
      if (bundle.workbook && typeof bundle.workbook.cancel === "function") {
        try { bundle.workbook.cancel(); } catch (_e) { /* noop */ }
      }
      if (bundle.workbookPath && fs.existsSync(bundle.workbookPath)) {
        try { fs.unlinkSync(bundle.workbookPath); } catch (_e) { /* noop */ }
      }
    }
    for (const wbBundle of splitWorkbookBundles) {
      if (wbBundle.workbook && typeof wbBundle.workbook.cancel === "function") {
        try { wbBundle.workbook.cancel(); } catch (_e) { /* noop */ }
      }
      if (wbBundle.workbookPath && fs.existsSync(wbBundle.workbookPath)) {
        try { fs.unlinkSync(wbBundle.workbookPath); } catch (_e) { /* noop */ }
      }
    }
    if (outputPath && fs.existsSync(outputPath)) {
      try { fs.unlinkSync(outputPath); } catch (_e) { /* noop */ }
    }
    if (batchDir && fs.existsSync(batchDir)) {
      try {
        const remaining = fs.readdirSync(batchDir);
        if (remaining.length === 0) fs.rmdirSync(batchDir);
      } catch (_e) {
        // noop
      }
    }
    if (error && error.code === "JOB_CANCELLED") {
      updateJob(jobId, { status: "cancelled", progress: 0, message: "Cancelled", error: null });
    } else {
      updateJob(jobId, {
        status: "failed",
        message: "Conversion failed",
        error: error.message || "Failed to process Excel file."
      });
    }
  }
}

async function processUploadJobForJsonLink(jobId, filePath, originalName) {
  let batchDir = "";
  let sourcePath = "";

  try {
    updateJob(jobId, { status: "processing", progress: 3, message: "Opening file..." });
    await ensureNotCancelled(jobId);
    updateJob(jobId, { progress: 10, message: "Preparing JSON link..." });

    const batchFolderName = getNextGenerateFolderName();
    batchDir = path.join(jsonDir, batchFolderName);
    fs.mkdirSync(batchDir, { recursive: true });
    updateJob(jobId, { outputFolder: batchFolderName });

    const sourceExt = path.extname(originalName || "").toLowerCase() || ".xlsx";
    const safeExt = sourceExt === ".xls" || sourceExt === ".xlsx" ? sourceExt : ".xlsx";
    sourcePath = path.join(batchDir, `source${safeExt}`);
    fs.renameSync(filePath, sourcePath);
    const accessToken = generateAccessToken();
    const metadata = await rebuildJsonFolderMetadata(batchFolderName, {
      jobId,
      token: accessToken,
      sourceFileName: path.basename(sourcePath),
      originalFileName: originalName
    });

    const sampleMunicipality = metadata.municipalityStats.length
      ? metadata.municipalityStats[0].municipality
      : "";
    const signedLink = buildSignedJsonLinkInfo(batchFolderName, metadata.token, sampleMunicipality);

    const result = {
      message: "JSON link generated successfully. Use a municipality name via ?sheet=.",
      rowCount: metadata.rowCount,
      outputMode: "json",
      workbookMode: "multiple",
      splitBy: metadata.splitBy || null,
      outputFolder: batchFolderName,
      fileCount: 1,
      municipalityCount: metadata.municipalityStats.length,
      municipalityStats: metadata.municipalityStats,
      jsonEndpointBase: signedLink.jsonEndpointBase,
      jsonUrlTemplate: signedLink.jsonUrlTemplate,
      sampleMunicipality,
      sampleUrl: signedLink.sampleUrl,
      urlExpiresAt: signedLink.expiresAt
    };

    updateJob(jobId, { status: "completed", progress: 100, message: "Done", result });
  } catch (error) {
    try { if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (_e) { /* noop */ }
    if (sourcePath && fs.existsSync(sourcePath)) {
      try { fs.unlinkSync(sourcePath); } catch (_e) { /* noop */ }
    }
    if (batchDir && fs.existsSync(batchDir)) {
      try {
        const remaining = fs.readdirSync(batchDir);
        if (remaining.length === 0) fs.rmdirSync(batchDir);
      } catch (_e) {
        // noop
      }
    }
    if (error && error.code === "JOB_CANCELLED") {
      updateJob(jobId, { status: "cancelled", progress: 0, message: "Cancelled", error: null });
    } else {
      updateJob(jobId, {
        status: "failed",
        message: "JSON link generation failed",
        error: error.message || "Failed to prepare JSON link."
      });
    }
  }
}

app.use(compression());
app.use(express.json({ limit: "1mb" }));
app.use((req, res, next) => {
  if (req.path === "/" || req.path.endsWith(".html") || req.path.startsWith("/progress/")) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.setHeader("Surrogate-Control", "no-store");
  }
  next();
});

app.use(express.static(projectRoot, { etag: false, lastModified: false, maxAge: 0 }));

app.get("/download/:folder/:file", async (req, res) => {
  const folder = String(req.params.folder || "").trim();
  const file = String(req.params.file || "").trim();
  if (!/^generate_\d+$/i.test(folder)) return res.status(400).json({ error: "Invalid folder." });
  if (!/\.zip$/i.test(file)) return res.status(400).json({ error: "Only ZIP downloads are allowed." });

  const folderPath = path.join(jsonDir, folder);
  const filePath = path.join(folderPath, file);
  const resolvedRoot = path.resolve(jsonDir);
  const resolvedFile = path.resolve(filePath);
  if (!resolvedFile.startsWith(`${resolvedRoot}${path.sep}`)) {
    return res.status(400).json({ error: "Invalid download path." });
  }
  try {
    const stat = await fsp.stat(resolvedFile);
    if (!stat.isFile()) return res.status(404).json({ error: "File not found." });
    return res.download(resolvedFile, path.basename(file));
  } catch (_e) {
    return res.status(404).json({ error: "File not found." });
  }
});

app.get("/admin", (_req, res) => {
  return res.sendFile(path.join(projectRoot, "admin.html"));
});

app.get("/admin/api/status", (req, res) => {
  if (!adminAuthEnabled) {
    return res.status(503).json({
      authenticated: false,
      enabled: false,
      error: "Admin auth is disabled. Configure ADMIN_USERNAME, ADMIN_PASSWORD, and ADMIN_SESSION_SECRET."
    });
  }
  const session = getAdminSession(req);
  return res.json({
    authenticated: Boolean(session),
    enabled: true,
    expiresAt: session ? session.expiresAt : null
  });
});

app.post("/admin/api/login", (req, res) => {
  if (!adminAuthEnabled) {
    return res.status(503).json({
      error: "Admin auth is disabled. Configure ADMIN_USERNAME, ADMIN_PASSWORD, and ADMIN_SESSION_SECRET."
    });
  }
  const ip = getRequestIp(req);
  if (isLoginRateLimited(ip)) {
    return res.status(429).json({ error: "Too many login attempts. Please try again later." });
  }
  const username = String(req.body && req.body.username ? req.body.username : "");
  const password = String(req.body && req.body.password ? req.body.password : "");
  if (!timingSafeStringEqual(username, ADMIN_USERNAME) || !timingSafeStringEqual(password, ADMIN_PASSWORD)) {
    registerFailedLoginAttempt(ip);
    clearAdminSessionCookie(res);
    return res.status(401).json({ error: "Invalid username or password." });
  }
  clearFailedLoginAttempts(ip);

  const session = createAdminSession();
  const maxAgeSeconds = Math.floor(ADMIN_SESSION_TTL_MS / 1000);
  res.setHeader("Set-Cookie", buildCookie(ADMIN_COOKIE_NAME, session.token, maxAgeSeconds));
  return res.json({ message: "Login successful.", expiresAt: session.expiresAt });
});

app.post("/admin/api/logout", requireAdminAuth, (req, res) => {
  clearAdminSessionCookie(res);
  return res.json({ message: "Logged out." });
});

app.get("/admin/api/folders", requireAdminAuth, async (_req, res) => {
  const activeOutputFolders = new Set(
    Array.from(jobs.values())
      .filter((job) => isActiveJobStatus(job.status) && job.outputFolder)
      .map((job) => String(job.outputFolder))
  );
  const baseFolders = listGenerateFolders();
  const folders = await Promise.all(baseFolders.map(async (f) => {
    const metadata = await loadFolderMetadataAsync(path.join(jsonDir, f.name));
    const token = metadata && metadata.token ? String(metadata.token) : "";
    const sampleMunicipality = metadata && Array.isArray(metadata.municipalityStats) && metadata.municipalityStats.length
      ? metadata.municipalityStats[0].municipality
      : "";
    const signed = token ? buildSignedJsonLinkInfo(f.name, token, sampleMunicipality) : null;
    return {
      name: f.name,
      modifiedAt: f.modifiedAt,
      fileCount: f.fileCount,
      dirCount: f.dirCount,
      sizeBytes: f.sizeBytes,
      activeJob: activeOutputFolders.has(f.name),
      outputMode: metadata && metadata.outputMode ? metadata.outputMode : null,
      rowCount: metadata && Number.isFinite(Number(metadata.rowCount)) ? Number(metadata.rowCount) : null,
      sourceFileName: metadata && metadata.sourceFileName ? metadata.sourceFileName : null,
      jsonUrlTemplate: signed ? signed.jsonUrlTemplate : null,
      sampleUrl: signed ? signed.sampleUrl : null,
      urlExpiresAt: signed ? signed.expiresAt : null,
      municipalityCount: metadata && Array.isArray(metadata.municipalityStats)
        ? metadata.municipalityStats.length
        : null
    };
  }));
  return res.json({ folders });
});

app.post("/admin/api/folders/:folderName/source/upload", requireAdminAuth, folderSourceUpload.single("sourceFile"), async (req, res) => {
  try {
    const folderName = String(req.params.folderName || "").trim();
    if (!/^generate_\d+$/i.test(folderName)) {
      try { if (req.file && req.file.path && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); } catch (_e) { /* noop */ }
      return res.status(400).json({ error: "Invalid folder name." });
    }
    const isActive = Array.from(jobs.values()).some(
      (job) => isActiveJobStatus(job.status) && String(job.outputFolder || "") === folderName
    );
    if (isActive) {
      try { if (req.file && req.file.path) await fsp.rm(req.file.path, { force: true }); } catch (_e) { /* noop */ }
      return res.status(409).json({ error: "Cannot modify folder while job is active." });
    }
    if (!req.file) return res.status(400).json({ error: "No source file uploaded." });
    const folderPath = path.join(jsonDir, folderName);
    let folderStat = null;
    try { folderStat = await fsp.stat(folderPath); } catch (_e) { folderStat = null; }
    if (!folderStat || !folderStat.isDirectory()) {
      try { if (req.file && req.file.path) await fsp.rm(req.file.path, { force: true }); } catch (_e) { /* noop */ }
      return res.status(404).json({ error: "Folder not found." });
    }

    const existing = await loadFolderMetadataAsync(folderPath);
    if (!existing || String(existing.outputMode || "") !== "json") {
      try { if (req.file && req.file.path) await fsp.rm(req.file.path, { force: true }); } catch (_e) { /* noop */ }
      return res.status(400).json({ error: "Folder does not contain JSON metadata." });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    const safeExt = ext === ".xls" || ext === ".xlsx" ? ext : ".xlsx";
    const sourceFileName = existing.sourceFileName || `source${safeExt}`;
    const targetPath = path.join(folderPath, sourceFileName);
    const tempTarget = `${targetPath}.tmp`;
    await fsp.copyFile(req.file.path, tempTarget);
    await fsp.rename(tempTarget, targetPath);
    try { await fsp.rm(req.file.path, { force: true }); } catch (_e) { /* noop */ }

    const metadata = await rebuildJsonFolderMetadata(folderName, {
      token: existing.token || generateAccessToken(),
      sourceFileName,
      originalFileName: req.file.originalname || sourceFileName
    });
    const sampleMunicipality = Array.isArray(metadata.municipalityStats) && metadata.municipalityStats.length
      ? metadata.municipalityStats[0].municipality
      : "";
    const signed = buildSignedJsonLinkInfo(folderName, metadata.token, sampleMunicipality);
    return res.json({
      message: `Source file updated for ${folderName}.`,
      rowCount: metadata.rowCount,
      municipalityCount: Array.isArray(metadata.municipalityStats) ? metadata.municipalityStats.length : 0,
      jsonUrlTemplate: signed.jsonUrlTemplate,
      sampleUrl: signed.sampleUrl,
      urlExpiresAt: signed.expiresAt
    });
  } catch (error) {
    try { if (req.file && req.file.path) await fsp.rm(req.file.path, { force: true }); } catch (_e) { /* noop */ }
    return res.status(500).json({ error: error.message || "Failed to update folder source." });
  }
});

app.post("/admin/api/folders/:folderName/refresh-caches", requireAdminAuth, async (req, res) => {
  try {
    const folderName = String(req.params.folderName || "").trim();
    if (!/^generate_\d+$/i.test(folderName)) {
      return res.status(400).json({ error: "Invalid folder name." });
    }
    const isActive = Array.from(jobs.values()).some(
      (job) => isActiveJobStatus(job.status) && String(job.outputFolder || "") === folderName
    );
    if (isActive) {
      return res.status(409).json({ error: "Cannot refresh folder while job is active." });
    }

    const folderPath = path.join(jsonDir, folderName);
    const existing = await loadFolderMetadataAsync(folderPath);
    if (!existing || String(existing.outputMode || "") !== "json") {
      return res.status(400).json({ error: "Folder does not contain JSON metadata." });
    }

    const metadata = await rebuildJsonFolderMetadata(folderName, {
      token: existing.token || generateAccessToken(),
      sourceFileName: existing.sourceFileName || "source.xlsx",
      originalFileName: existing.originalFileName || existing.sourceFileName || "source.xlsx"
    });
    const sampleMunicipality = Array.isArray(metadata.municipalityStats) && metadata.municipalityStats.length
      ? metadata.municipalityStats[0].municipality
      : "";
    const signed = buildSignedJsonLinkInfo(folderName, metadata.token, sampleMunicipality);
    return res.json({
      message: `Refreshed ${folderName} metadata and cleared caches.`,
      rowCount: metadata.rowCount,
      municipalityCount: Array.isArray(metadata.municipalityStats) ? metadata.municipalityStats.length : 0,
      jsonUrlTemplate: signed.jsonUrlTemplate,
      sampleUrl: signed.sampleUrl,
      urlExpiresAt: signed.expiresAt
    });
  } catch (error) {
    return res.status(500).json({ error: error.message || "Failed to refresh folder caches." });
  }
});

app.post("/admin/api/folders/:folderName/rotate-token", requireAdminAuth, async (req, res) => {
  try {
    const folderName = String(req.params.folderName || "").trim();
    if (!/^generate_\d+$/i.test(folderName)) {
      return res.status(400).json({ error: "Invalid folder name." });
    }
    const isActive = Array.from(jobs.values()).some(
      (job) => isActiveJobStatus(job.status) && String(job.outputFolder || "") === folderName
    );
    if (isActive) {
      return res.status(409).json({ error: "Cannot rotate token while job is active." });
    }
    const folderPath = path.join(jsonDir, folderName);
    const existing = await loadFolderMetadataAsync(folderPath);
    if (!existing || String(existing.outputMode || "") !== "json") {
      return res.status(400).json({ error: "Folder does not contain JSON metadata." });
    }
    const newToken = generateAccessToken();
    const updated = { ...existing, token: newToken, updatedAt: Date.now() };
    await fsp.writeFile(path.join(folderPath, "metadata.json"), JSON.stringify(updated));
    const sampleMunicipality = Array.isArray(updated.municipalityStats) && updated.municipalityStats.length
      ? updated.municipalityStats[0].municipality
      : "";
    const signed = buildSignedJsonLinkInfo(folderName, newToken, sampleMunicipality);
    return res.json({
      message: `Token rotated for ${folderName}.`,
      jsonUrlTemplate: signed.jsonUrlTemplate,
      sampleUrl: signed.sampleUrl,
      urlExpiresAt: signed.expiresAt
    });
  } catch (error) {
    return res.status(500).json({ error: error.message || "Failed to rotate token." });
  }
});

app.post("/admin/api/folders/:folderName/revoke-token", requireAdminAuth, async (req, res) => {
  try {
    const folderName = String(req.params.folderName || "").trim();
    if (!/^generate_\d+$/i.test(folderName)) {
      return res.status(400).json({ error: "Invalid folder name." });
    }
    const isActive = Array.from(jobs.values()).some(
      (job) => isActiveJobStatus(job.status) && String(job.outputFolder || "") === folderName
    );
    if (isActive) {
      return res.status(409).json({ error: "Cannot revoke token while job is active." });
    }
    const folderPath = path.join(jsonDir, folderName);
    const existing = await loadFolderMetadataAsync(folderPath);
    if (!existing || String(existing.outputMode || "") !== "json") {
      return res.status(400).json({ error: "Folder does not contain JSON metadata." });
    }
    const newToken = generateAccessToken();
    const updated = { ...existing, token: newToken, updatedAt: Date.now() };
    await fsp.writeFile(path.join(folderPath, "metadata.json"), JSON.stringify(updated));
    const sampleMunicipality = Array.isArray(updated.municipalityStats) && updated.municipalityStats.length
      ? updated.municipalityStats[0].municipality
      : "";
    const signed = buildSignedJsonLinkInfo(folderName, newToken, sampleMunicipality);
    return res.json({
      message: `Old token revoked and replaced for ${folderName}.`,
      jsonUrlTemplate: signed.jsonUrlTemplate,
      sampleUrl: signed.sampleUrl,
      urlExpiresAt: signed.expiresAt
    });
  } catch (error) {
    return res.status(500).json({ error: error.message || "Failed to revoke token." });
  }
});

app.delete("/admin/api/folders/:folderName", requireAdminAuth, (req, res) => {
  const folderName = String(req.params.folderName || "").trim();
  if (!/^generate_\d+$/i.test(folderName)) {
    return res.status(400).json({ error: "Invalid folder name." });
  }

  const confirm = String(req.body && req.body.confirm ? req.body.confirm : "").trim();
  if (confirm !== folderName) {
    return res.status(400).json({ error: "Confirmation mismatch. Send { \"confirm\": \"<folder>\" }." });
  }

  const isActive = Array.from(jobs.values()).some(
    (job) => isActiveJobStatus(job.status) && String(job.outputFolder || "") === folderName
  );
  if (isActive) {
    return res.status(409).json({ error: "Cannot delete folder while job is active." });
  }

  const dirPath = path.join(jsonDir, folderName);
  const resolvedRoot = path.resolve(jsonDir);
  const resolvedPath = path.resolve(dirPath);
  if (!resolvedPath.startsWith(`${resolvedRoot}${path.sep}`) && resolvedPath !== resolvedRoot) {
    return res.status(400).json({ error: "Invalid target path." });
  }
  if (!fs.existsSync(resolvedPath)) {
    return res.status(404).json({ error: "Folder not found." });
  }
  if (!fs.statSync(resolvedPath).isDirectory()) {
    return res.status(400).json({ error: "Target is not a folder." });
  }

  fs.rmSync(resolvedPath, { recursive: true, force: true });
  return res.json({ message: `Deleted ${folderName}.` });
});

app.get("/excel/json/:folder/:token", async (req, res) => {
  try {
    const folder = String(req.params.folder || "").trim();
    const token = String(req.params.token || "").trim();
    const requestedSheet = normalizeMunicipality(req.query && req.query.sheet ? req.query.sheet : "");
    const expRaw = String(req.query && req.query.exp ? req.query.exp : "").trim();
    const sig = String(req.query && req.query.sig ? req.query.sig : "").trim();
    if (!folder || !token) return res.status(400).json({ error: "Invalid JSON link." });
    if (!requestedSheet) return res.status(400).json({ error: "Missing required query parameter: sheet" });
    if (!JSON_LINK_SIGNING_SECRET) return res.status(503).json({ error: "JSON link signing secret is not configured." });
    const isNonExpiringFolder = NON_EXPIRING_JSON_FOLDERS.has(folder);
    let expectedSig = "";
    if (isNonExpiringFolder && !expRaw) {
      expectedSig = createJsonLinkSignature(folder, token, "never");
    } else {
      const exp = Number(expRaw);
      if (!Number.isInteger(exp) || exp <= Math.floor(Date.now() / 1000)) {
        return res.status(401).json({ error: "JSON link expired or invalid." });
      }
      expectedSig = createJsonLinkSignature(folder, token, exp);
    }
    if (!timingSafeStringEqual(sig, expectedSig)) {
      return res.status(401).json({ error: "Invalid JSON link signature." });
    }

    const batchDir = path.join(jsonDir, folder);
    const metadataPath = path.join(batchDir, "metadata.json");
    try { await fsp.access(metadataPath, fs.constants.F_OK); } catch (_e) { return res.status(404).json({ error: "JSON link not found." }); }

    let metadata = JSON.parse(await fsp.readFile(metadataPath, "utf8"));
    if (!metadata || metadata.token !== token) return res.status(404).json({ error: "JSON link not found." });
    if (metadata.outputMode !== "json") return res.status(400).json({ error: "This link does not point to JSON mode output." });

    const sourcePath = path.join(batchDir, String(metadata.sourceFileName || "source.xlsx"));
    try { await fsp.access(sourcePath, fs.constants.F_OK); } catch (_e) {
      return res.status(404).json({ error: "Source file for this folder is missing." });
    }
    let sourceMtimeMs = 0;
    try { sourceMtimeMs = Number((await fsp.stat(sourcePath)).mtimeMs || 0); } catch (_e) { sourceMtimeMs = 0; }
    const metadataMtime = Number(metadata.sourceMtimeMs || 0);
    if (sourceMtimeMs && sourceMtimeMs !== metadataMtime) {
      metadata = await rebuildJsonFolderMetadata(folder, {
        token: metadata.token || generateAccessToken(),
        sourceFileName: metadata.sourceFileName || "source.xlsx",
        originalFileName: metadata.originalFileName || metadata.sourceFileName || "source.xlsx"
      });
    }

    const knownMunicipalities = new Set(
      Array.isArray(metadata.municipalityStats)
        ? metadata.municipalityStats.map((m) => getMunicipalityCacheKey(m && m.municipality ? m.municipality : ""))
        : []
    );

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");

    const requestedKey = getMunicipalityCacheKey(requestedSheet);
    if (knownMunicipalities.size > 0 && !knownMunicipalities.has(requestedKey)) {
      await writeChunk(
        res,
        `{"sheet":${JSON.stringify(requestedSheet)},"splitBy":${JSON.stringify(metadata.splitBy || "MUNICIPALITY")},"rows":[],"rowCount":0}`
      );
      return res.end();
    }

    const cacheInfo = await ensureMunicipalityJsonCache(batchDir, metadata, requestedSheet);
    await writeChunk(
      res,
      `{"sheet":${JSON.stringify(requestedSheet)},"splitBy":${JSON.stringify(metadata.splitBy || "MUNICIPALITY")},"rows":[`
    );
    const streamedCount = await streamJsonRowsArrayFromJsonl(res, cacheInfo.cachePath);
    const finalCount = Number(cacheInfo.rowCount) || streamedCount;
    await writeChunk(res, `],"rowCount":${finalCount}}`);
    return res.end();
  } catch (error) {
    if (!res.headersSent) {
      return res.status(500).json({ error: error.message || "Failed to generate JSON response." });
    }
    return res.end();
  }
});

app.get("/progress/:jobId", async (req, res) => {
  const jobId = String(req.params.jobId || "");
  const job = await getJob(jobId);
  if (!job) return res.status(404).json({ error: "Job not found." });
  return res.json({
    jobId,
    status: job.status,
    progress: job.progress,
    message: job.message,
    currentMunicipality: job.currentMunicipality || null,
    currentMunicipalityRows: job.currentMunicipalityRows || 0,
    currentWorkbookPart: job.currentWorkbookPart || null,
    processedRows: job.processedRows || 0,
    cancelRequested: Boolean(job.cancelRequested),
    result: job.result || null,
    error: job.error || null
  });
});

app.post("/cancel/:jobId", async (req, res) => {
  const jobId = String(req.params.jobId || "");
  const job = await getJob(jobId);
  if (!job) return res.status(404).json({ error: "Job not found." });
  if (!isActiveJobStatus(job.status)) {
    return res.json({ message: `Job already ${job.status}.`, jobId, status: job.status });
  }
  updateJob(jobId, { cancelRequested: true, message: "Cancelling..." });
  await setRemoteJob(jobId, { ...job, cancelRequested: true, message: "Cancelling...", updatedAt: Date.now() });
  return res.json({ message: "Cancellation requested.", jobId, status: "cancelling" });
});

app.post("/upload", upload.single("excelFile"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "No file uploaded." });
    const workbookModeRaw = String(req.body && req.body.workbookMode ? req.body.workbookMode : "single")
      .trim()
      .toLowerCase();
    if (!["single", "multiple", "split"].includes(workbookModeRaw)) {
      return res.status(400).json({ error: "Invalid workbook mode. Use 'single', 'multiple', or 'split'." });
    }
    const outputModeRaw = String(req.body && req.body.outputMode ? req.body.outputMode : "zip")
      .trim()
      .toLowerCase();
    if (!["zip", "json"].includes(outputModeRaw)) {
      return res.status(400).json({ error: "Invalid output mode. Use 'zip' or 'json'." });
    }
    if (outputModeRaw === "json" && workbookModeRaw !== "multiple") {
      return res.status(400).json({ error: "JSON mode is only available for Multiple Workbook." });
    }
    const splitCountRaw = req.body && req.body.splitCount ? req.body.splitCount : "2";
    const splitCount = normalizeSplitCount(splitCountRaw);
    const jobId = crypto.randomUUID();
    const newJob = {
      status: "queued",
      progress: 0,
      message: "Queued",
      currentMunicipality: null,
      currentMunicipalityRows: 0,
      processedRows: 0,
      cancelRequested: false,
      workbookMode: workbookModeRaw,
      outputMode: outputModeRaw,
      splitCount,
      result: null,
      error: null,
      createdAt: Date.now(),
      updatedAt: Date.now()
    };
    jobs.set(jobId, newJob);
    await setRemoteJob(jobId, newJob);
    processUploadJob(jobId, req.file.path, req.file.originalname, workbookModeRaw, splitCount, outputModeRaw);
    return res.status(202).json({ message: "Upload received. Conversion started.", jobId });
  } catch (error) {
    return res.status(500).json({ error: error.message || "Failed to process Excel file." });
  }
});

app.use((error, _req, res, _next) => {
  if (error instanceof multer.MulterError) return res.status(400).json({ error: error.message });
  if (error) return res.status(400).json({ error: error.message });
  return res.status(500).json({ error: "Unexpected server error." });
});

if (!isVercelRuntime) {
  app.listen(port, () => {
    console.log(`Excel municipality generator running at http://localhost:${port}`);
  });
}

module.exports = app;
