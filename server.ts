import express, { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-Memory Distributed Tracing Storage & Event Store
interface Span {
  service: string;
  spanId: string;
  parentSpanId?: string;
  traceId: string;
  name: string;
  startTimeMs: number;
  durationMs: number;
  attributes: Record<string, string | number | boolean>;
  status: 'OK' | 'ERROR';
}

interface TraceRecord {
  traceId: string;
  scenario: string;
  timestamp: string;
  spans: Span[];
  logs: Array<{
    timestamp: string;
    level: string;
    service: string;
    traceId: string;
    spanId: string;
    message: string;
    mdc: Record<string, string>;
  }>;
}

const traceStore: Map<string, TraceRecord> = new Map();
const globalLogs: Array<any> = [];

// Salt for HIPAA-compliant pseudonymization of Patient Identifiers (MRN)
const AUDIT_SALT = process.env.HIS_AUDIT_SALT || 'salt_k9#xL92@HIS_SECURE_SALT';

function pseudonymizeMrn(mrn: string): string {
  if (!mrn) return 'ANONYMOUS';
  return crypto.createHmac('sha256', AUDIT_SALT).update(mrn).digest('hex').substring(0, 16);
}

// =========================================================================
// MIDDLEWARE: W3C Trace Context Generator / Injector (Spring Cloud Gateway Emulator)
// =========================================================================
app.use((req: Request, res: Response, next: NextFunction) => {
  const inboundTraceparent = req.header('traceparent');
  const inboundXTraceId = req.header('x-trace-id') || req.header('x-correlation-id');
  const w3cRegex = /^00-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})-([0-9a-fA-F]{2})$/;

  let traceId: string;
  let spanId: string;
  let outgoingTraceparent: string;

  if (inboundTraceparent && w3cRegex.test(inboundTraceparent)) {
    const match = inboundTraceparent.match(w3cRegex);
    traceId = match![1];
    spanId = crypto.randomBytes(8).toString('hex'); // 16 hex chars for downstream child span
    outgoingTraceparent = `00-${traceId}-${spanId}-01`;
  } else if (inboundXTraceId && /^[0-9a-fA-F]{16,32}$/.test(inboundXTraceId)) {
    traceId = inboundXTraceId;
    spanId = crypto.randomBytes(8).toString('hex');
    outgoingTraceparent = `00-${traceId.padStart(32, '0')}-${spanId}-01`;
  } else {
    // Generate fresh W3C identifiers: 32 hex for traceId, 16 hex for spanId
    traceId = crypto.randomBytes(16).toString('hex');
    spanId = crypto.randomBytes(8).toString('hex');
    outgoingTraceparent = `00-${traceId}-${spanId}-01`;
  }

  // Inject into request context & headers
  req.headers['traceparent'] = outgoingTraceparent;
  req.headers['x-correlation-id'] = traceId;
  req.headers['x-trace-id'] = traceId;
  (req as any).traceContext = {
    traceId,
    spanId,
    traceparent: outgoingTraceparent,
    startTime: Date.now()
  };

  // Attach X-Trace-Id & X-Correlation-ID to client response headers
  res.setHeader('X-Trace-Id', traceId);
  res.setHeader('X-Correlation-ID', traceId);
  res.setHeader('traceparent', outgoingTraceparent);

  next();
});

// Helper to log with Logback MDC format
function logMdc(service: string, traceId: string, spanId: string, level: string, message: string, mdcExtra: Record<string, string> = {}) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    level,
    service,
    traceId,
    spanId,
    message,
    mdc: {
      traceId,
      spanId,
      ...mdcExtra
    }
  };
  globalLogs.unshift(logEntry);
  if (globalLogs.length > 500) globalLogs.pop();

  const record = traceStore.get(traceId);
  if (record) {
    record.logs.push(logEntry);
  }

  console.log(
    `[${logEntry.timestamp}] ${level.padEnd(5)} [${service},${traceId},${spanId}] [${JSON.stringify(mdcExtra)}] : ${message}`
  );
  return logEntry;
}

// =========================================================================
// BACKEND API ENDPOINTS
// =========================================================================

// Health check
app.get('/api/health', (req: Request, res: Response) => {
  res.json({
    status: 'UP',
    standard: 'OpenTelemetry W3C Trace Context',
    springBootTarget: '3.4.0',
    javaVersion: '17',
    storedTraces: traceStore.size,
    uptimeSeconds: Math.floor(process.uptime())
  });
});

// Ingress 1: ER Trauma Admission (Gateway -> Admission Service -> Chart Service -> Kafka)
app.post(['/api/v1/er/triage-admissions', '/api/v1/admissions', '/api/gateway/er/admissions', '/api/api/gateway/er/admissions'], async (req: Request, res: Response) => {
  const ctx = (req as any).traceContext;
  const { patientMRN = 'MRN-902148', triageLevel = 'ESI-1', ward = 'TRAUMA-BAY-3', attendingDoctorId = 'DOC-4482' } = req.body;
  const patientHash = pseudonymizeMrn(patientMRN);
  const encounterId = `ENC-${Date.now().toString().slice(-6)}`;

  const record: TraceRecord = {
    traceId: ctx.traceId,
    scenario: 'Emergency Room (ER) Patient Triage & Admission',
    timestamp: new Date().toISOString(),
    spans: [],
    logs: []
  };
  traceStore.set(ctx.traceId, record);

  // 1. Gateway Ingress Span
  const gatewaySpan: Span = {
    service: 'his-api-gateway',
    name: 'POST /api/v1/er/triage-admissions',
    spanId: ctx.spanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now(),
    durationMs: 12,
    attributes: {
      'http.method': 'POST',
      'http.status_code': 200,
      'http.route': '/api/v1/er/triage-admissions'
    },
    status: 'OK'
  };
  record.spans.push(gatewaySpan);
  logMdc('his-api-gateway', ctx.traceId, ctx.spanId, 'INFO', `Inbound ER Trauma request accepted. Routing downstream to lb://patient-admission-service`);

  // 2. Microservice 1: patient-admission-service
  const admissionSpanId = crypto.randomBytes(8).toString('hex');
  const admissionSpan: Span = {
    service: 'patient-admission-service',
    name: 'AdmissionController.createAdmission',
    spanId: admissionSpanId,
    parentSpanId: ctx.spanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 15,
    durationMs: 38,
    attributes: {
      'his.encounter_id': encounterId,
      'his.patient_hash': patientHash,
      'his.triage_level': triageLevel,
      'his.ward': ward
    },
    status: 'OK'
  };
  record.spans.push(admissionSpan);
  logMdc('patient-admission-service', ctx.traceId, admissionSpanId, 'INFO', `Ingress ER admission request. Extracted traceparent from Gateway headers.`, {
    encounterId,
    patientIdHash: patientHash
  });
  logMdc('patient-admission-service', ctx.traceId, admissionSpanId, 'INFO', `Trauma bed reserved: ${ward}. Dispatching RestClient call to clinical-chart-service`, {
    encounterId,
    patientIdHash: patientHash
  });

  // 3. Microservice 2: clinical-chart-service (Synchronous HTTP via RestClient)
  const chartSpanId = crypto.randomBytes(8).toString('hex');
  const chartSpan: Span = {
    service: 'clinical-chart-service',
    name: 'ChartController.initiateEncounter',
    spanId: chartSpanId,
    parentSpanId: admissionSpanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 55,
    durationMs: 24,
    attributes: {
      'his.encounter_id': encounterId,
      'his.attending_md': attendingDoctorId
    },
    status: 'OK'
  };
  record.spans.push(chartSpan);
  logMdc('clinical-chart-service', ctx.traceId, chartSpanId, 'INFO', `Encounter chart initialized for patient token [${patientHash}]. Linked Attending MD [${attendingDoctorId}]`, {
    encounterId
  });

  // 4. Asynchronous Kafka Event Propagation: topic his.er.events
  const kafkaSpanId = crypto.randomBytes(8).toString('hex');
  const kafkaSpan: Span = {
    service: 'bedside-telemetry-service',
    name: 'KafkaConsumer: his.er.events',
    spanId: kafkaSpanId,
    parentSpanId: admissionSpanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 85,
    durationMs: 45,
    attributes: {
      'messaging.system': 'kafka',
      'messaging.destination': 'his.er.events',
      'messaging.kafka.partition': 2,
      'his.encounter_id': encounterId
    },
    status: 'OK'
  };
  record.spans.push(kafkaSpan);
  logMdc('bedside-telemetry-service', ctx.traceId, kafkaSpanId, 'INFO', `Kafka Record consumed with preserved W3C headers. Bedside monitor telemetry initialized in ${ward}`, {
    encounterId,
    partition: '2'
  });

  res.status(200).json({
    message: 'ER Admission registered successfully',
    traceId: ctx.traceId,
    traceparent: ctx.traceparent,
    correlationId: ctx.traceId,
    encounterId,
    patientPseudonym: patientHash,
    hopsCompleted: record.spans.length,
    spans: record.spans
  });
});

// Ingress 2: Controlled Substance Prescription (EPCS DEA Compliance Flow)
app.post(['/api/v1/pharmacy/prescriptions', '/api/gateway/prescriptions', '/api/api/gateway/prescriptions'], async (req: Request, res: Response) => {
  const ctx = (req as any).traceContext;
  const { rxId = `RX-${Date.now().toString().slice(-6)}`, drugCode = 'NDC-0054-0235-25 (Morphine Sulfate 15mg)', prescriberDea = 'XD9921401' } = req.body;

  const record: TraceRecord = {
    traceId: ctx.traceId,
    scenario: 'Schedule II Controlled Substance E-Prescription (EPCS)',
    timestamp: new Date().toISOString(),
    spans: [],
    logs: []
  };
  traceStore.set(ctx.traceId, record);

  // Gateway
  const gatewaySpan: Span = {
    service: 'his-api-gateway',
    name: 'POST /api/v1/pharmacy/prescriptions',
    spanId: ctx.spanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now(),
    durationMs: 14,
    attributes: { 'http.route': '/api/v1/pharmacy/prescriptions' },
    status: 'OK'
  };
  record.spans.push(gatewaySpan);
  logMdc('his-api-gateway', ctx.traceId, ctx.spanId, 'INFO', `Ingress EPCS prescription. Injected root traceparent.`);

  // E-Prescription Service
  const rxSpanId = crypto.randomBytes(8).toString('hex');
  const rxSpan: Span = {
    service: 'eprescription-service',
    name: 'PrescriptionController.signOrder',
    spanId: rxSpanId,
    parentSpanId: ctx.spanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 18,
    durationMs: 62,
    attributes: { 'his.rx_id': rxId, 'dea.prescriber': prescriberDea },
    status: 'OK'
  };
  record.spans.push(rxSpan);
  logMdc('eprescription-service', ctx.traceId, rxSpanId, 'INFO', `DEA EPCS identity validation verified for Prescriber [${prescriberDea}]. Calling Drug Interaction Service via RestClient`, {
    rxId
  });

  // Drug Interaction Service
  const interactionSpanId = crypto.randomBytes(8).toString('hex');
  const interactionSpan: Span = {
    service: 'drug-interaction-service',
    name: 'InteractionController.evaluate',
    spanId: interactionSpanId,
    parentSpanId: rxSpanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 82,
    durationMs: 31,
    attributes: { 'his.drug_code': drugCode, 'his.interaction_risk': 'LOW' },
    status: 'OK'
  };
  record.spans.push(interactionSpan);
  logMdc('drug-interaction-service', ctx.traceId, interactionSpanId, 'INFO', `Cross-referenced active medications against ${drugCode}. Contraindications: NONE`, {
    rxId
  });

  // Kafka DEA Audit Sink
  const deaAuditSpanId = crypto.randomBytes(8).toString('hex');
  const deaAuditSpan: Span = {
    service: 'dea-audit-service',
    name: 'KafkaConsumer: his.dea.audit',
    spanId: deaAuditSpanId,
    parentSpanId: rxSpanId,
    traceId: ctx.traceId,
    startTimeMs: Date.now() + 115,
    durationMs: 39,
    attributes: { 'messaging.system': 'kafka', 'messaging.destination': 'his.dea.audit' },
    status: 'OK'
  };
  record.spans.push(deaAuditSpan);
  logMdc('dea-audit-service', ctx.traceId, deaAuditSpanId, 'INFO', `DEA Compliance audit ledger updated with tamper-proof signature for Rx [${rxId}].`, {
    rxId
  });

  res.status(200).json({
    message: 'Controlled substance e-prescription submitted and audited',
    traceId: ctx.traceId,
    traceparent: ctx.traceparent,
    correlationId: ctx.traceId,
    rxId,
    hopsCompleted: record.spans.length,
    spans: record.spans
  });
});

// Retrieve specific trace with all spans and MDC logs
app.get('/api/traces/:traceId', (req: Request, res: Response) => {
  const { traceId } = req.params;
  const trace = traceStore.get(traceId);
  if (!trace) {
    return res.status(404).json({ error: 'Trace not found', traceId });
  }
  res.json(trace);
});

// List all active traces
app.get('/api/traces', (req: Request, res: Response) => {
  const traces = Array.from(traceStore.values()).map(t => ({
    traceId: t.traceId,
    scenario: t.scenario,
    timestamp: t.timestamp,
    spansCount: t.spans.length,
    services: Array.from(new Set(t.spans.map(s => s.service))),
    totalDurationMs: t.spans.reduce((acc, s) => acc + s.durationMs, 0)
  }));
  res.json({ total: traces.length, traces });
});

// Retrieve MDC unified logs
app.get('/api/logs', (req: Request, res: Response) => {
  const { traceId, limit = '100' } = req.query;
  let logs = globalLogs;
  if (traceId) {
    logs = logs.filter(l => l.traceId === traceId);
  }
  res.json({
    total: logs.length,
    logs: logs.slice(0, parseInt(limit as string, 10))
  });
});

// Tool: Validate / Generate W3C Traceparent
app.post('/api/tools/parse-traceparent', (req: Request, res: Response) => {
  const { traceparent } = req.body;
  if (!traceparent) {
    return res.status(400).json({ error: 'traceparent header string required' });
  }

  const parts = traceparent.trim().split('-');
  if (parts.length !== 4) {
    return res.status(400).json({ valid: false, error: 'Expected 4 hyphen-delimited fields' });
  }

  const [version, traceId, parentSpanId, flags] = parts;
  const valid = version === '00' && /^[0-9a-fA-F]{32}$/.test(traceId) && /^[0-9a-fA-F]{16}$/.test(parentSpanId) && /^[0-9a-fA-F]{2}$/.test(flags);

  res.json({
    valid,
    version,
    traceId,
    parentSpanId,
    flags,
    isSampled: flags === '01',
    standard: 'W3C Trace Context (RFC Recommendation)'
  });
});

// =========================================================================
// VITE INTEGRATION (Mount Vite middleware for dev / static in prod)
// =========================================================================
// =========================================================================
// PURE BACKEND ROOT & DOCUMENTATION (100% Backend)
// =========================================================================
import fs from 'fs';

app.get('/', (req: Request, res: Response) => {
  const readmePath = path.resolve(__dirname, 'README.md');
  const readmeContent = fs.existsSync(readmePath)
    ? fs.readFileSync(readmePath, 'utf-8')
    : 'HIS Distributed Tracing Backend Engine';

  // If client accepts JSON or curl, return backend status
  if (req.headers['accept']?.includes('application/json')) {
    return res.json({
      status: 'UP',
      system: 'Hospital Information System (HIS) Distributed Tracing Engine',
      architecture: '100% Backend (Spring Boot 3.4.0, Java 17, Spring Cloud Gateway, Micrometer, W3C)',
      endpoints: [
        'POST /api/v1/er/triage-admissions',
        'POST /api/gateway/er/admissions',
        'POST /api/gateway/prescriptions',
        'GET /api/traces/:traceId',
        'GET /api/traces',
        'GET /api/logs',
        'GET /api/health',
        'POST /api/tools/parse-traceparent',
        'GET /readme'
      ]
    });
  }

  // Render raw markdown / formatted backend documentation
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>HIS Distributed Tracing Backend Engine</title>
  <style>
    body { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; background: #020617; color: #f8fafc; padding: 2rem; max-width: 900px; margin: 0 auto; line-height: 1.6; }
    h1, h2, h3 { color: #38bdf8; border-bottom: 1px solid #1e293b; padding-bottom: 0.3rem; }
    code, pre { background: #0f172a; color: #7dd3fc; padding: 0.2rem 0.4rem; border-radius: 4px; font-size: 0.85rem; border: 1px solid #1e293b; }
    pre { padding: 1rem; overflow-x: auto; white-space: pre-wrap; }
    .badge { display: inline-block; padding: 0.2rem 0.5rem; background: #064e3b; color: #34d399; border-radius: 4px; font-size: 0.75rem; font-weight: bold; margin-bottom: 1rem; }
    a { color: #38bdf8; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .endpoint { background: #0f172a; border-left: 3px solid #38bdf8; padding: 0.5rem 1rem; margin: 0.5rem 0; }
  </style>
</head>
<body>
  <div class="badge">100% BACKEND • SPRING BOOT 3.4.0 • JAVA 17 • W3C TRACE CONTEXT</div>
  <h1>HIS Distributed Tracing Architecture Engine</h1>
  <p>This application is 100% backend. All frontend code has been removed. The active microservices architecture files are located in <code>/backend-java/</code> and the backend REST API is running directly on this server.</p>

  <h2>Available Backend Endpoints</h2>
  <div class="endpoint"><strong>POST /api/v1/er/triage-admissions</strong> - Ingests ER trauma request, generates/injects W3C traceparent, propagates to clinical-chart-service and Kafka</div>
  <div class="endpoint"><strong>POST /api/gateway/prescriptions</strong> - Submits EPCS DEA controlled substance order with salted patientIdHash audit trail</div>
  <div class="endpoint"><strong>GET /api/traces/:traceId</strong> - Inspects full distributed span tree across Gateway, HTTP Clients, and Kafka</div>
  <div class="endpoint"><strong>GET /api/logs</strong> - Streams unified Logback MDC logs correlated by Trace ID</div>
  <div class="endpoint"><strong>GET /api/health</strong> - Observability health check</div>
  <div class="endpoint"><strong>GET /readme</strong> - Raw README markdown text</div>

  <h2>Documentation & Verification Guide</h2>
  <pre>${readmeContent.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</pre>
</body>
</html>`);
});

app.get('/readme', (req: Request, res: Response) => {
  const readmePath = path.resolve(__dirname, 'README.md');
  if (fs.existsSync(readmePath)) {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(fs.readFileSync(readmePath, 'utf-8'));
  } else {
    res.status(404).send('README.md not found');
  }
});

function startServer() {
  app.listen(PORT, () => {
    console.log(`[HIS-DISTRIBUTED-TRACING-BACKEND] 100% Backend Engine listening on port ${PORT}`);
    console.log(`[INFO] REST endpoints live: /api/gateway/er/admissions, /api/traces, /api/logs, /readme`);
  });
}

startServer();
