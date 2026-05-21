import express from "express";
import mysql from "mysql2/promise";
import os from "os";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = Number(process.env.PORT || 80);

const CLOUD_PROVIDER = process.env.CLOUD_PROVIDER || "UNKNOWN";
const REGION_NAME = process.env.REGION_NAME || "UNKNOWN";
const APP_COLOR = process.env.APP_COLOR || "#111827";

const DB_ENGINE_NAME = process.env.DB_ENGINE_NAME || "MySQL";
const DB_HOST = process.env.DB_HOST;
const DB_PORT = Number(process.env.DB_PORT || 3306);
const DB_USER = process.env.DB_USER || "appuser";
const DB_PASSWORD = process.env.DB_PASSWORD || "ChangeMe123!";
const DB_NAME = process.env.DB_NAME || "activeapp";
const DB_SSL = String(process.env.DB_SSL || "false").toLowerCase() === "true";

const REPLICATION_SECRET = process.env.REPLICATION_SECRET || "ChangeThis-Replication-Secret";

const ORDER_OWNER_BASE_URL = process.env.ORDER_OWNER_BASE_URL || "";
const INVENTORY_OWNER_BASE_URL = process.env.INVENTORY_OWNER_BASE_URL || "";

const REMOTE_ORDER_REPLICATION_URL = process.env.REMOTE_ORDER_REPLICATION_URL || "";
const REMOTE_INVENTORY_REPLICATION_URL = process.env.REMOTE_INVENTORY_REPLICATION_URL || "";

const isAws = CLOUD_PROVIDER.toLowerCase() === "aws";
const isAzure = CLOUD_PROVIDER.toLowerCase() === "azure";

let pool;

function newEventId(prefix) {
  return `${prefix}-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`;
}

function newOrderId() {
  const ymd = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  return `ORD-${ymd}-${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

function getClientIp(req) {
  return String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown");
}

async function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USER,
      password: DB_PASSWORD,
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      enableKeepAlive: true,
      connectTimeout: 8000,
      ssl: DB_SSL ? { rejectUnauthorized: false } : undefined
    });
  }
  return pool;
}

async function initDb() {
  const p = await getPool();

  await p.execute(`
    CREATE TABLE IF NOT EXISTS orders_source (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      order_id VARCHAR(80) NOT NULL UNIQUE,
      customer_name VARCHAR(100) NOT NULL,
      product_code VARCHAR(80) NOT NULL,
      quantity INT NOT NULL,
      status VARCHAR(40) NOT NULL,
      source_cloud VARCHAR(40) NOT NULL,
      origin_server VARCHAR(255) NOT NULL,
      event_id VARCHAR(120) NOT NULL UNIQUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await p.execute(`
    CREATE TABLE IF NOT EXISTS orders_replica (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      order_id VARCHAR(80) NOT NULL UNIQUE,
      customer_name VARCHAR(100) NOT NULL,
      product_code VARCHAR(80) NOT NULL,
      quantity INT NOT NULL,
      status VARCHAR(40) NOT NULL,
      source_cloud VARCHAR(40) NOT NULL,
      origin_server VARCHAR(255) NOT NULL,
      event_id VARCHAR(120) NOT NULL UNIQUE,
      source_created_at DATETIME NULL,
      replicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await p.execute(`
    CREATE TABLE IF NOT EXISTS inventory_source (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      product_code VARCHAR(80) NOT NULL UNIQUE,
      product_name VARCHAR(160) NOT NULL,
      available_qty INT NOT NULL,
      reserved_qty INT NOT NULL DEFAULT 0,
      source_cloud VARCHAR(40) NOT NULL,
      event_id VARCHAR(120) NULL UNIQUE,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await p.execute(`
    CREATE TABLE IF NOT EXISTS inventory_replica (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      product_code VARCHAR(80) NOT NULL UNIQUE,
      product_name VARCHAR(160) NOT NULL,
      available_qty INT NOT NULL,
      reserved_qty INT NOT NULL DEFAULT 0,
      source_cloud VARCHAR(40) NOT NULL,
      event_id VARCHAR(120) NOT NULL UNIQUE,
      source_updated_at DATETIME NULL,
      replicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await p.execute(`
    CREATE TABLE IF NOT EXISTS event_outbox (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      event_id VARCHAR(120) NOT NULL UNIQUE,
      event_type VARCHAR(80) NOT NULL,
      direction VARCHAR(40) NOT NULL,
      target_url TEXT NOT NULL,
      payload_json JSON NOT NULL,
      status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      sent_at DATETIME NULL
    )
  `);

  if (isAzure) {
    await p.execute(`
      INSERT INTO inventory_source
        (product_code, product_name, available_qty, reserved_qty, source_cloud, event_id)
      VALUES
        ('SKU-AWS-LAB', 'AWS 실습권', 30, 0, 'Azure', 'seed-SKU-AWS-LAB'),
        ('SKU-AZURE-BOOK', 'Azure 교재', 40, 0, 'Azure', 'seed-SKU-AZURE-BOOK'),
        ('SKU-MULTICLOUD-PASS', '멀티클라우드 패스', 25, 0, 'Azure', 'seed-SKU-MULTICLOUD-PASS'),
        ('SKU-SECURITY-KIT', '보안 실습 키트', 15, 0, 'Azure', 'seed-SKU-SECURITY-KIT')
      ON DUPLICATE KEY UPDATE
        product_name = VALUES(product_name)
    `);
  }
}

async function postJson(url, payload) {
  if (!url) {
    return { ok: false, skipped: true, reason: "target url is empty" };
  }

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-replication-secret": REPLICATION_SECRET
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();

  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}: ${JSON.stringify(body)}`);
    err.status = res.status;
    err.body = body;
    throw err;
  }

  return body;
}

async function saveOutboxAndTrySend({ eventId, eventType, direction, targetUrl, payload }) {
  const p = await getPool();

  await p.execute(
    `
    INSERT INTO event_outbox
      (event_id, event_type, direction, target_url, payload_json, status, attempts)
    VALUES
      (?, ?, ?, ?, CAST(? AS JSON), 'PENDING', 0)
    ON DUPLICATE KEY UPDATE
      payload_json = VALUES(payload_json),
      target_url = VALUES(target_url)
    `,
    [eventId, eventType, direction, targetUrl, JSON.stringify(payload)]
  );

  try {
    const result = await postJson(targetUrl, payload);

    await p.execute(
      `
      UPDATE event_outbox
      SET status = 'SENT',
          attempts = attempts + 1,
          last_error = NULL,
          sent_at = NOW()
      WHERE event_id = ?
      `,
      [eventId]
    );

    return { ok: true, result };
  } catch (err) {
    await p.execute(
      `
      UPDATE event_outbox
      SET status = 'FAILED',
          attempts = attempts + 1,
          last_error = ?
      WHERE event_id = ?
      `,
      [err.message, eventId]
    );

    return { ok: false, error: err.message };
  }
}

function requireReplicationSecret(req, res, next) {
  const actual = req.headers["x-replication-secret"];

  if (actual !== REPLICATION_SECRET) {
    return res.status(401).json({
      ok: false,
      message: "Invalid replication secret"
    });
  }

  next();
}

async function proxyToOwner(baseUrl, path, body) {
  if (!baseUrl) {
    throw new Error(`Owner base URL is empty for path ${path}`);
  }

  const res = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-replication-secret": REPLICATION_SECRET,
      "x-proxy-from": CLOUD_PROVIDER
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(`Owner API failed: ${res.status}`);
    err.body = json;
    throw err;
  }

  return json;
}

app.get("/health", async (req, res) => {
  try {
    const p = await getPool();
    await p.query("SELECT 1 AS ok");

    res.json({
      ok: true,
      cloudProvider: CLOUD_PROVIDER,
      regionName: REGION_NAME,
      serverName: os.hostname(),
      dbEngine: DB_ENGINE_NAME,
      dbHost: DB_HOST,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      cloudProvider: CLOUD_PROVIDER,
      error: err.message
    });
  }
});

app.post("/api/orders", async (req, res) => {
  if (!isAws) {
    try {
      const result = await proxyToOwner(ORDER_OWNER_BASE_URL, "/api/orders", req.body);
      return res.json({
        ok: true,
        proxiedTo: "AWS order owner",
        result
      });
    } catch (err) {
      return res.status(502).json({
        ok: false,
        message: "Azure는 주문 원본 소유자가 아니므로 AWS로 프록시했지만 실패했습니다.",
        error: err.message,
        detail: err.body
      });
    }
  }

  const p = await getPool();

  const customerName = req.body.customerName || "주일사마님";
  const productCode = req.body.productCode || "SKU-AZURE-BOOK";
  const quantity = Number(req.body.quantity || 1);

  const orderId = newOrderId();
  const eventId = newEventId("aws-order-created");

  await p.execute(
    `
    INSERT INTO orders_source
      (order_id, customer_name, product_code, quantity, status, source_cloud, origin_server, event_id)
    VALUES
      (?, ?, ?, ?, 'ORDER_ACCEPTED', 'AWS', ?, ?)
    `,
    [orderId, customerName, productCode, quantity, os.hostname(), eventId]
  );

  const eventPayload = {
    eventId,
    eventType: "OrderCreated",
    sourceCloud: "AWS",
    order: {
      orderId,
      customerName,
      productCode,
      quantity,
      status: "ORDER_ACCEPTED",
      sourceCloud: "AWS",
      originServer: os.hostname(),
      createdAt: new Date().toISOString()
    }
  };

  const replication = await saveOutboxAndTrySend({
    eventId,
    eventType: "OrderCreated",
    direction: "AWS_TO_AZURE",
    targetUrl: REMOTE_ORDER_REPLICATION_URL,
    payload: eventPayload
  });

  res.status(201).json({
    ok: true,
    owner: "AWS",
    message: "주문은 AWS RDS orders_source에 저장되었습니다.",
    order: eventPayload.order,
    replication
  });
});

app.post("/api/inventory/reserve", async (req, res) => {
  if (!isAzure) {
    try {
      const result = await proxyToOwner(INVENTORY_OWNER_BASE_URL, "/api/inventory/reserve", req.body);
      return res.json({
        ok: true,
        proxiedTo: "Azure inventory owner",
        result
      });
    } catch (err) {
      return res.status(502).json({
        ok: false,
        message: "AWS는 재고 원본 소유자가 아니므로 Azure로 프록시했지만 실패했습니다.",
        error: err.message,
        detail: err.body
      });
    }
  }

  const p = await getPool();

  const productCode = req.body.productCode || "SKU-AZURE-BOOK";
  const quantity = Number(req.body.quantity || 1);

  const [rows] = await p.query(
    `
    SELECT product_code, product_name, available_qty, reserved_qty
    FROM inventory_source
    WHERE product_code = ?
    `,
    [productCode]
  );

  if (rows.length === 0) {
    return res.status(404).json({
      ok: false,
      message: "해당 상품 코드가 Azure inventory_source에 없습니다.",
      productCode
    });
  }

  const current = rows[0];

  if (current.available_qty < quantity) {
    return res.status(409).json({
      ok: false,
      message: "재고가 부족합니다.",
      productCode,
      availableQty: current.available_qty,
      requestedQty: quantity
    });
  }

  const eventId = newEventId("azure-inventory-changed");

  await p.execute(
    `
    UPDATE inventory_source
    SET available_qty = available_qty - ?,
        reserved_qty = reserved_qty + ?,
        event_id = ?
    WHERE product_code = ?
    `,
    [quantity, quantity, eventId, productCode]
  );

  const [updatedRows] = await p.query(
    `
    SELECT product_code, product_name, available_qty, reserved_qty, updated_at
    FROM inventory_source
    WHERE product_code = ?
    `,
    [productCode]
  );

  const updated = updatedRows[0];

  const eventPayload = {
    eventId,
    eventType: "InventoryChanged",
    sourceCloud: "Azure",
    inventory: {
      productCode: updated.product_code,
      productName: updated.product_name,
      availableQty: updated.available_qty,
      reservedQty: updated.reserved_qty,
      sourceCloud: "Azure",
      originServer: os.hostname(),
      updatedAt: new Date(updated.updated_at).toISOString()
    }
  };

  const replication = await saveOutboxAndTrySend({
    eventId,
    eventType: "InventoryChanged",
    direction: "AZURE_TO_AWS",
    targetUrl: REMOTE_INVENTORY_REPLICATION_URL,
    payload: eventPayload
  });

  res.json({
    ok: true,
    owner: "Azure",
    message: "재고는 Azure MySQL inventory_source에서 차감되었습니다.",
    inventory: eventPayload.inventory,
    replication
  });
});

app.post("/replication/orders/upsert", requireReplicationSecret, async (req, res) => {
  if (!isAzure) {
    return res.json({
      ok: true,
      ignored: true,
      message: "이 엔드포인트는 Azure orders_replica 반영용입니다."
    });
  }

  const payload = req.body;
  const order = payload.order;

  if (!payload.eventId || !order?.orderId) {
    return res.status(400).json({
      ok: false,
      message: "Invalid order replication payload"
    });
  }

  const p = await getPool();

  await p.execute(
    `
    INSERT INTO orders_replica
      (order_id, customer_name, product_code, quantity, status, source_cloud, origin_server, event_id, source_created_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      customer_name = VALUES(customer_name),
      product_code = VALUES(product_code),
      quantity = VALUES(quantity),
      status = VALUES(status),
      origin_server = VALUES(origin_server),
      replicated_at = CURRENT_TIMESTAMP
    `,
    [
      order.orderId,
      order.customerName,
      order.productCode,
      Number(order.quantity || 1),
      order.status || "ORDER_ACCEPTED",
      order.sourceCloud || "AWS",
      order.originServer || "unknown",
      payload.eventId,
      order.createdAt ? new Date(order.createdAt) : null
    ]
  );

  res.json({
    ok: true,
    target: "Azure orders_replica",
    eventId: payload.eventId,
    orderId: order.orderId
  });
});

app.post("/replication/inventory/upsert", requireReplicationSecret, async (req, res) => {
  if (!isAws) {
    return res.json({
      ok: true,
      ignored: true,
      message: "이 엔드포인트는 AWS inventory_replica 반영용입니다."
    });
  }

  const payload = req.body;
  const inventory = payload.inventory;

  if (!payload.eventId || !inventory?.productCode) {
    return res.status(400).json({
      ok: false,
      message: "Invalid inventory replication payload"
    });
  }

  const p = await getPool();

  await p.execute(
    `
    INSERT INTO inventory_replica
      (product_code, product_name, available_qty, reserved_qty, source_cloud, event_id, source_updated_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      product_name = VALUES(product_name),
      available_qty = VALUES(available_qty),
      reserved_qty = VALUES(reserved_qty),
      source_cloud = VALUES(source_cloud),
      event_id = VALUES(event_id),
      source_updated_at = VALUES(source_updated_at),
      replicated_at = CURRENT_TIMESTAMP
    `,
    [
      inventory.productCode,
      inventory.productName,
      Number(inventory.availableQty || 0),
      Number(inventory.reservedQty || 0),
      inventory.sourceCloud || "Azure",
      payload.eventId,
      inventory.updatedAt ? new Date(inventory.updatedAt) : null
    ]
  );

  res.json({
    ok: true,
    target: "AWS inventory_replica",
    eventId: payload.eventId,
    productCode: inventory.productCode
  });
});

app.get("/api/state", async (req, res) => {
  const p = await getPool();

  const [ordersSource] = await p.query(`
    SELECT order_id, customer_name, product_code, quantity, status, source_cloud, origin_server, created_at
    FROM orders_source
    ORDER BY id DESC
    LIMIT 10
  `);

  const [ordersReplica] = await p.query(`
    SELECT order_id, customer_name, product_code, quantity, status, source_cloud, origin_server, replicated_at
    FROM orders_replica
    ORDER BY id DESC
    LIMIT 10
  `);

  const [inventorySource] = await p.query(`
    SELECT product_code, product_name, available_qty, reserved_qty, source_cloud, updated_at
    FROM inventory_source
    ORDER BY product_code ASC
  `);

  const [inventoryReplica] = await p.query(`
    SELECT product_code, product_name, available_qty, reserved_qty, source_cloud, replicated_at
    FROM inventory_replica
    ORDER BY product_code ASC
  `);

  const [outbox] = await p.query(`
    SELECT event_id, event_type, direction, status, attempts, last_error, created_at, sent_at
    FROM event_outbox
    ORDER BY id DESC
    LIMIT 10
  `);

  res.json({
    ok: true,
    cloudProvider: CLOUD_PROVIDER,
    serverName: os.hostname(),
    tables: {
      ordersSource,
      ordersReplica,
      inventorySource,
      inventoryReplica,
      outbox
    }
  });
});

app.post("/api/admin/retry-outbox", async (req, res) => {
  const p = await getPool();

  const [events] = await p.query(`
    SELECT event_id, event_type, direction, target_url, payload_json
    FROM event_outbox
    WHERE status IN ('PENDING', 'FAILED')
    ORDER BY id ASC
    LIMIT 20
  `);

  const results = [];

  for (const ev of events) {
    try {
      const payload = typeof ev.payload_json === "string"
        ? JSON.parse(ev.payload_json)
        : ev.payload_json;

      const result = await postJson(ev.target_url, payload);

      await p.execute(
        `
        UPDATE event_outbox
        SET status = 'SENT',
            attempts = attempts + 1,
            last_error = NULL,
            sent_at = NOW()
        WHERE event_id = ?
        `,
        [ev.event_id]
      );

      results.push({ eventId: ev.event_id, ok: true, result });
    } catch (err) {
      await p.execute(
        `
        UPDATE event_outbox
        SET status = 'FAILED',
            attempts = attempts + 1,
            last_error = ?
        WHERE event_id = ?
        `,
        [err.message, ev.event_id]
      );

      results.push({ eventId: ev.event_id, ok: false, error: err.message });
    }
  }

  res.json({
    ok: true,
    retried: results.length,
    results
  });
});

app.get("/", async (req, res) => {
  res.setHeader("content-type", "text/html; charset=utf-8");

  const cloudIcon = isAws ? "🟠" : isAzure ? "🔵" : "⚪";
  const ownerText = isAws
    ? "주문 원본 소유자 · Azure 재고 복제본 조회"
    : "재고 원본 소유자 · AWS 주문 복제본 조회";

  res.end(`
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Active-Active Multi-Cloud DB Sync</title>
  <style>
    :root {
      --bg:#020617;
      --card:rgba(255,255,255,.12);
      --line:rgba(255,255,255,.18);
      --text:#f8fafc;
      --muted:#cbd5e1;
      --accent:${APP_COLOR};
      --ok:#34d399;
      --bad:#fb7185;
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      min-height:100vh;
      color:var(--text);
      font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      background:
        radial-gradient(circle at 18% 20%, ${APP_COLOR}99, transparent 28%),
        radial-gradient(circle at 82% 12%, #38bdf866, transparent 26%),
        radial-gradient(circle at 50% 100%, #22c55e44, transparent 30%),
        linear-gradient(135deg,#020617,#111827);
    }
    main {
      width:min(1180px, calc(100% - 32px));
      margin:0 auto;
      padding:44px 0;
    }
    .hero,.card {
      border:1px solid var(--line);
      background:linear-gradient(180deg,rgba(255,255,255,.16),rgba(255,255,255,.08));
      backdrop-filter:blur(20px);
      box-shadow:0 30px 90px rgba(0,0,0,.36);
    }
    .hero {
      border-radius:32px;
      padding:34px;
      margin-bottom:18px;
    }
    h1 {
      font-size:clamp(34px,5vw,64px);
      letter-spacing:-.06em;
      line-height:1.04;
      margin:18px 0 10px;
    }
    .badge {
      display:inline-flex;
      border:1px solid var(--line);
      border-radius:999px;
      padding:10px 14px;
      color:var(--muted);
      background:rgba(0,0,0,.22);
    }
    .muted { color:var(--muted); line-height:1.6; }
    .grid {
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:18px;
    }
    @media(max-width:900px){ .grid{grid-template-columns:1fr;} }
    .card {
      border-radius:26px;
      padding:22px;
      overflow:hidden;
    }
    .kpis {
      display:grid;
      grid-template-columns:repeat(4,1fr);
      gap:12px;
      margin-top:20px;
    }
    @media(max-width:900px){ .kpis{grid-template-columns:1fr 1fr;} }
    @media(max-width:560px){ .kpis{grid-template-columns:1fr;} }
    .kpi {
      border:1px solid var(--line);
      border-radius:20px;
      padding:16px;
      background:rgba(0,0,0,.22);
    }
    .label { color:var(--muted); font-size:13px; margin-bottom:6px; }
    .value { font-size:20px; font-weight:900; word-break:break-word; }
    input,select {
      width:100%;
      border:1px solid var(--line);
      background:rgba(0,0,0,.22);
      color:var(--text);
      border-radius:16px;
      padding:13px;
      outline:none;
      margin:6px 0 12px;
    }
    button {
      border:0;
      border-radius:16px;
      padding:13px 15px;
      color:white;
      font-weight:900;
      cursor:pointer;
      background:linear-gradient(135deg,var(--accent),#2563eb);
      margin:4px 4px 4px 0;
    }
    button.secondary { background:linear-gradient(135deg,#334155,#64748b); }
    button.warn { background:linear-gradient(135deg,#f59e0b,#ef4444); }
    pre {
      white-space:pre-wrap;
      word-break:break-word;
      background:rgba(0,0,0,.26);
      border:1px solid var(--line);
      border-radius:18px;
      padding:14px;
      max-height:420px;
      overflow:auto;
      font-size:13px;
      color:#dbeafe;
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="badge">🌐 Active-Active Multi-Cloud · ${cloudIcon} ${CLOUD_PROVIDER}</div>
      <h1>${cloudIcon} ${CLOUD_PROVIDER}<br/>쓰기 소유권 분리 DB 동기화</h1>
      <p class="muted">
        ${ownerText}<br/>
        주문은 AWS가 원본이고, 재고는 Azure가 원본입니다.
        반대편 DB에는 필요한 데이터만 replica 테이블에 이벤트로 복제합니다.
      </p>
      <div class="kpis">
        <div class="kpi"><div class="label">Cloud</div><div class="value">${CLOUD_PROVIDER}</div></div>
        <div class="kpi"><div class="label">Region</div><div class="value">${REGION_NAME}</div></div>
        <div class="kpi"><div class="label">Server</div><div class="value">${os.hostname()}</div></div>
        <div class="kpi"><div class="label">DB</div><div class="value">${DB_ENGINE_NAME}</div></div>
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>🛒 주문 생성</h2>
        <p class="muted">주문 원본은 항상 AWS RDS orders_source에 저장됩니다.</p>
        <label>고객명</label>
        <input id="customerName" value="주일사마님" />
        <label>상품 코드</label>
        <select id="orderProductCode">
          <option value="SKU-AZURE-BOOK">SKU-AZURE-BOOK · Azure 교재</option>
          <option value="SKU-AWS-LAB">SKU-AWS-LAB · AWS 실습권</option>
          <option value="SKU-MULTICLOUD-PASS">SKU-MULTICLOUD-PASS · 멀티클라우드 패스</option>
          <option value="SKU-SECURITY-KIT">SKU-SECURITY-KIT · 보안 실습 키트</option>
        </select>
        <label>수량</label>
        <input id="orderQuantity" type="number" min="1" value="1" />
        <button onclick="createOrder()">AWS 주문 원본 생성</button>
      </div>

      <div class="card">
        <h2>📦 재고 차감</h2>
        <p class="muted">재고 원본은 항상 Azure MySQL inventory_source에서 차감됩니다.</p>
        <label>상품 코드</label>
        <select id="inventoryProductCode">
          <option value="SKU-AZURE-BOOK">SKU-AZURE-BOOK · Azure 교재</option>
          <option value="SKU-AWS-LAB">SKU-AWS-LAB · AWS 실습권</option>
          <option value="SKU-MULTICLOUD-PASS">SKU-MULTICLOUD-PASS · 멀티클라우드 패스</option>
          <option value="SKU-SECURITY-KIT">SKU-SECURITY-KIT · 보안 실습 키트</option>
        </select>
        <label>차감 수량</label>
        <input id="inventoryQuantity" type="number" min="1" value="1" />
        <button onclick="reserveInventory()">Azure 재고 원본 차감</button>
      </div>
    </section>

    <section class="card" style="margin-top:18px;">
      <h2>📊 상태 확인</h2>
      <button class="secondary" onclick="loadState()">DB 상태 새로고침</button>
      <button class="warn" onclick="retryOutbox()">실패 이벤트 재전송</button>
      <pre id="output">대기 중입니다.</pre>
    </section>
  </main>

<script>
async function callJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: { "content-type": "application/json", ...(options.headers || {}) }
  });

  const text = await res.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { raw: text }; }

  if (!res.ok) {
    throw { status: res.status, body };
  }

  return body;
}

function show(data) {
  document.getElementById("output").textContent = JSON.stringify(data, null, 2);
}

async function createOrder() {
  try {
    const body = {
      customerName: document.getElementById("customerName").value,
      productCode: document.getElementById("orderProductCode").value,
      quantity: Number(document.getElementById("orderQuantity").value || 1)
    };

    const result = await callJson("/api/orders", {
      method: "POST",
      body: JSON.stringify(body)
    });

    show(result);
  } catch (err) {
    show(err);
  }
}

async function reserveInventory() {
  try {
    const body = {
      productCode: document.getElementById("inventoryProductCode").value,
      quantity: Number(document.getElementById("inventoryQuantity").value || 1)
    };

    const result = await callJson("/api/inventory/reserve", {
      method: "POST",
      body: JSON.stringify(body)
    });

    show(result);
  } catch (err) {
    show(err);
  }
}

async function loadState() {
  try {
    const result = await callJson("/api/state");
    show(result);
  } catch (err) {
    show(err);
  }
}

async function retryOutbox() {
  try {
    const result = await callJson("/api/admin/retry-outbox", { method: "POST" });
    show(result);
  } catch (err) {
    show(err);
  }
}

loadState();
</script>
</body>
</html>
  `);
});

initDb()
  .then(() => {
    app.listen(PORT, "0.0.0.0", () => {
      console.log(`${CLOUD_PROVIDER} Active-Active Sync App listening on ${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Startup failed:", err);
    process.exit(1);
  });
