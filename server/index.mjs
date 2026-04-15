import { createServer } from "node:http";

const PORT = Number(process.env.PORT || 8787);
const trackedOrders = new Map();

function json(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
  });
  res.end(JSON.stringify(payload));
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 1_000_000) {
        reject(new Error("Body too large"));
      }
    });
    req.on("end", () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        reject(new Error("Invalid JSON payload"));
      }
    });
    req.on("error", reject);
  });
}

function validateOrderPayload(payload) {
  const link = String(payload.link || "").trim();
  const servicesPayload = payload.services && typeof payload.services === "object" ? payload.services : null;
  const serviceEntries = servicesPayload
    ? Object.entries(servicesPayload).filter(([, value]) => value && typeof value === "object")
    : [];

  if (!serviceEntries.length) {
    const apiUrl = normalizePanelUrl(payload.apiUrl);
    const apiKey = String(payload.apiKey || "").trim();
    const service = String(payload.service || "").trim();
    const runs = Array.isArray(payload.runs) ? payload.runs : [];
    if (!apiUrl) return "API URL must not be empty";
    if (!apiKey) return "API key must not be empty";
    if (!service) return "Service ID must not be empty";
    if (!runs.length) return "Runs must be a non-empty array";
    for (const [index, run] of runs.entries()) {
      const quantity = Number(run?.quantity);
      const time = String(run?.time || "").trim();
      if (!Number.isFinite(quantity) || quantity <= 0) return `Run ${index + 1} quantity must be valid`;
      if (!time) return `Run ${index + 1} time is required`;
      const parsedTime = new Date(time);
      if (Number.isNaN(parsedTime.getTime())) return `Run ${index + 1} time is invalid`;
    }
    try {
      const parsedApiUrl = new URL(apiUrl);
      if (parsedApiUrl.protocol !== "http:" && parsedApiUrl.protocol !== "https:") {
        return "API URL must be a valid URL";
      }
    } catch {
      return "API URL must be a valid URL";
    }
  } else {
    for (const [label, serviceConfig] of serviceEntries) {
      const config = serviceConfig;
      const apiUrl = normalizePanelUrl(config.apiUrl || payload.apiUrl);
      const apiKey = String(config.apiKey || payload.apiKey || "").trim();
      const serviceId = String(config.serviceId || "").trim();
      const runs = Array.isArray(config.runs) ? config.runs : [];
      if (!apiUrl) return `${label} API URL must not be empty`;
      if (!apiKey) return `${label} API key must not be empty`;
      if (!serviceId) return `${label} service ID must not be empty`;
      if (!runs.length) return `${label} runs must be a non-empty array`;
      for (const [index, run] of runs.entries()) {
        const quantity = Number(run?.quantity);
        const time = String(run?.time || "").trim();
        if (!Number.isFinite(quantity) || quantity <= 0) return `${label} run ${index + 1} quantity must be valid`;
        if (!time) return `${label} run ${index + 1} time is required`;
        const parsedTime = new Date(time);
        if (Number.isNaN(parsedTime.getTime())) return `${label} run ${index + 1} time is invalid`;
      }
      try {
        const parsedApiUrl = new URL(apiUrl);
        if (parsedApiUrl.protocol !== "http:" && parsedApiUrl.protocol !== "https:") {
          return `${label} API URL must be a valid URL`;
        }
      } catch {
        return `${label} API URL must be a valid URL`;
      }
    }
  }

  try {
    const parsedUrl = new URL(link);
    if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
      return "Link must be a valid URL";
    }
  } catch {
    return "Link must be a valid URL";
  }

  return null;
}

function maskKey(key) {
  const value = String(key || "");
  if (!value) return "";
  if (value.length <= 6) return `${"*".repeat(Math.max(0, value.length - 2))}${value.slice(-2)}`;
  return `${value.slice(0, 2)}${"*".repeat(value.length - 4)}${value.slice(-2)}`;
}

function normalizePanelUrl(rawUrl) {
  const trimmed = String(rawUrl || "").trim();
  if (!trimmed) return "";

  const withProtocol = /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  const parsed = new URL(withProtocol);

  // YoYoMedia requires requests to /api/v2; default it when users save only the host URL.
  if (parsed.hostname.toLowerCase() === "yoyomedia.in" && (parsed.pathname === "/" || parsed.pathname === "")) {
    parsed.pathname = "/api/v2";
  }

  return parsed.toString();
}

function recalculateOrderProgress(order) {
  if (!order) return;
  if (order.status === "cancelled" || order.status === "paused" || order.status === "failed") {
    order.completedRuns = order.runs.filter((run) => run.status === "completed").length;
    return;
  }

  const now = Date.now();
  for (const run of order.runs) {
    if (run.status === "pending" && run.time <= now) {
      run.status = "completed";
    }
  }

  order.completedRuns = order.runs.filter((run) => run.status === "completed").length;
  if (order.completedRuns >= order.runs.length) {
    order.status = "completed";
  } else if (order.status !== "paused") {
    order.status = "running";
  }
}

createServer(async (req, res) => {
  const requestUrl = new URL(req.url || "/", "http://localhost");
  const pathname = requestUrl.pathname.replace(/\/$/, "") || "/";

  if (req.method === "OPTIONS") {
    return json(res, 200, { ok: true });
  }

  if (req.method !== "POST" || (pathname !== "/api/order" && pathname !== "/api/services" && pathname !== "/api/order/control")) {
    return json(res, 404, { success: false, error: "Not found" });
  }

  try {
    const payload = await readJsonBody(req);
    console.info(`[POST ${pathname}] Incoming request`, {
      at: new Date().toISOString(),
      apiUrl: String(payload.apiUrl || "").trim(),
    });

    if (pathname === "/api/order/control") {
      const schedulerOrderId = String(payload.schedulerOrderId || "").trim();
      const action = String(payload.action || "").trim();
      if (!schedulerOrderId) {
        return json(res, 400, { success: false, error: "schedulerOrderId is required" });
      }
      if (action !== "pause" && action !== "resume" && action !== "cancel") {
        return json(res, 400, { success: false, error: "action must be pause, resume, or cancel" });
      }

      const trackedOrder = trackedOrders.get(schedulerOrderId);
      if (!trackedOrder) {
        return json(res, 404, { success: false, error: "Tracked order not found" });
      }

      recalculateOrderProgress(trackedOrder);
      if (action === "pause" && trackedOrder.status !== "cancelled" && trackedOrder.status !== "completed") {
        trackedOrder.status = "paused";
      }
      if (action === "resume" && trackedOrder.status !== "cancelled" && trackedOrder.status !== "completed") {
        trackedOrder.status = "running";
      }
      if (action === "cancel" && trackedOrder.status !== "completed") {
        trackedOrder.status = "cancelled";
        trackedOrder.runs = trackedOrder.runs.map((run) => ({
          ...run,
          status: run.status === "pending" ? "cancelled" : run.status,
        }));
      }

      trackedOrder.completedRuns = trackedOrder.runs.filter((run) => run.status === "completed").length;
      return json(res, 200, {
        success: true,
        schedulerOrderId,
        status: trackedOrder.status,
        completedRuns: trackedOrder.completedRuns,
        runStatuses: trackedOrder.runs.map((run) => run.status),
      });
    }

    if (pathname === "/api/services") {
      const apiUrl = normalizePanelUrl(payload.apiUrl);
      const apiKey = String(payload.apiKey || "").trim();
      console.info("[POST /api/services] Incoming body", {
        apiUrl,
        key: maskKey(apiKey),
      });
      if (!apiUrl) return json(res, 400, { success: false, error: "API URL must not be empty" });
      if (!apiKey) return json(res, 400, { success: false, error: "API key must not be empty" });
      try {
        const parsed = new URL(apiUrl);
        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
          return json(res, 400, { success: false, error: "API URL must be a valid URL" });
        }
      } catch {
        return json(res, 400, { success: false, error: "API URL must be a valid URL" });
      }

      const requestBody = new URLSearchParams({
        key: apiKey,
        action: "services",
      }).toString();

      console.info("[POST /api/services] Provider request body", {
        apiUrl,
        action: "services",
        key: maskKey(apiKey),
      });

      const apiResponse = await fetch(apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: requestBody,
      });

      const text = await apiResponse.text();
      console.info("[POST /api/services] Provider response", {
        status: apiResponse.status,
        bodyPreview: text.slice(0, 500),
      });

      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = null;
      }

      if (!apiResponse.ok) {
        return json(res, 502, {
          success: false,
          error: parsed?.error || `SMM panel HTTP ${apiResponse.status} while fetching services`,
        });
      }

      if (parsed?.error) {
        return json(res, 400, {
          success: false,
          error: String(parsed.error),
        });
      }

      const services = Array.isArray(parsed)
        ? parsed
        : Array.isArray(parsed?.services)
          ? parsed.services
          : Array.isArray(parsed?.data)
            ? parsed.data
            : [];

      if (!Array.isArray(services)) {
        return json(res, 502, {
          success: false,
          error: "SMM panel returned an invalid services payload",
          raw: parsed ?? text,
        });
      }

      return json(res, 200, {
        success: true,
        services,
      });
    }

    console.info("[POST /api/order] Order payload summary", {
      service: String(payload.service || "").trim(),
      runs: Array.isArray(payload.runs) ? payload.runs.length : 0,
      serviceGroups: payload.services && typeof payload.services === "object" ? Object.keys(payload.services) : [],
      link: String(payload.link || "").trim(),
    });

    const validationError = validateOrderPayload(payload);
    if (validationError) {
      return json(res, 400, { success: false, error: validationError });
    }

    const serviceGroups = payload.services && typeof payload.services === "object" ? payload.services : null;
    const serviceEntries = serviceGroups
      ? Object.entries(serviceGroups)
          .filter(([, value]) => value && typeof value === "object")
          .map(([label, value]) => ({
            label,
            apiUrl: normalizePanelUrl(value.apiUrl || payload.apiUrl),
            apiKey: String(value.apiKey || payload.apiKey),
            serviceId: String(value.serviceId),
            runs: Array.isArray(value.runs) ? value.runs : [],
          }))
      : [
          {
            label: "views",
            apiUrl: normalizePanelUrl(payload.apiUrl),
            apiKey: String(payload.apiKey),
            serviceId: String(payload.service),
            runs: Array.isArray(payload.runs) ? payload.runs : [],
          },
        ];

    const orderIds = [];
    const schedulerOrderId = `sch-${Date.now()}-${Math.floor(Math.random() * 10_000)}`;

    for (const entry of serviceEntries) {
      console.log("SERVICE:", entry.label, entry.apiUrl);

      for (let index = 0; index < entry.runs.length; index += 1) {
        const run = entry.runs[index];
        const quantity = String(Math.floor(Number(run.quantity)));
        const requestBody = new URLSearchParams({
          key: entry.apiKey,
          action: "add",
          service: entry.serviceId,
          link: String(payload.link),
          quantity,
        }).toString();

        console.info("[POST /api/order] Provider request body", {
          serviceLabel: entry.label,
          apiUrl: entry.apiUrl,
          action: "add",
          service: entry.serviceId,
          quantity,
          runIndex: index + 1,
          runTime: String(run.time || ""),
          key: maskKey(entry.apiKey),
        });

        const apiResponse = await fetch(entry.apiUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: requestBody,
        });

        const text = await apiResponse.text();
        console.info("[POST /api/order] Provider response", {
          serviceLabel: entry.label,
          status: apiResponse.status,
          runIndex: index + 1,
          bodyPreview: text.slice(0, 500),
        });

        let parsed;
        try {
          parsed = JSON.parse(text);
        } catch {
          parsed = null;
        }

        if (!apiResponse.ok) {
          return json(res, 502, {
            success: false,
            error: parsed?.error || `SMM panel HTTP ${apiResponse.status}`,
            service: entry.label,
            run: index + 1,
          });
        }

        if (parsed?.error) {
          return json(res, 400, {
            success: false,
            error: String(parsed.error),
            service: entry.label,
            run: index + 1,
            raw: parsed,
          });
        }

        if (parsed?.order === undefined) {
          return json(res, 502, {
            success: false,
            error: "SMM panel did not return an order ID",
            service: entry.label,
            run: index + 1,
            raw: parsed ?? text,
          });
        }

        orderIds.push(String(parsed.order));
      }
    }

    const trackingRunsSource = serviceEntries.find((entry) => entry.label === "views")?.runs || serviceEntries[0]?.runs || [];
    const trackedRuns = trackingRunsSource.map((run) => ({
      time: new Date(run.time).getTime(),
      quantity: Number(run.quantity),
      status: "pending",
    }));
    const trackedOrder = {
      id: schedulerOrderId,
      name: String(payload.name || ""),
      status: "running",
      runs: trackedRuns,
      completedRuns: 0,
      createdAt: Date.now(),
    };
    recalculateOrderProgress(trackedOrder);
    trackedOrders.set(schedulerOrderId, trackedOrder);

    return json(res, 200, {
      success: true,
      orderId: orderIds[0],
      orderIds,
      schedulerOrderId,
      status: trackedOrder.status,
      completedRuns: trackedOrder.completedRuns,
      runStatuses: trackedOrder.runs.map((run) => run.status),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Internal server error";
    return json(res, 500, { success: false, error: message });
  }
}).listen(PORT, () => {
  // Simple runtime log for local backend start.
  console.log(`Dev SMM API server listening on http://localhost:${PORT}`);
});
