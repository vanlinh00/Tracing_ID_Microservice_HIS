# Distributed Tracing with Unique Trace IDs in Healthcare Information Systems (HIS)

An enterprise architecture and backend implementation guide for distributed tracing across clinical microservices using **Java 17**, **Spring Boot 3.4.0**, **Spring Cloud Gateway (2024.x)**, **Micrometer Tracing**, and **OpenTelemetry W3C Trace Context (`traceparent`)**.

---

## 1. Core Architectural Concept

In a Hospital Information System (HIS), clinical transactions (such as ER Trauma Triage or Controlled Substance E-Prescribing) traverse multiple service boundaries:
```
[Edge Client / EHR]
        │
        ▼ (POST /api/v1/er/admissions)
┌──────────────────────────────┐
│     Spring Cloud Gateway     │  <-- Generates/Validates Root Trace ID (32-hex)
└──────────────┬───────────────┘      Injects W3C 'traceparent' & 'X-Correlation-ID'
               │
               ▼ HTTP (RestClient) with 'traceparent: 00-{traceId}-{spanId}-01'
┌──────────────────────────────┐
│  patient-admission-service   │  <-- Extracts trace into Logback MDC
└──────┬────────────────┬──────┘      Computes salted patientIdHash (HIPAA Safe Harbor)
       │                │
       │ HTTP           │ Kafka (RecordHeaders)
       ▼                ▼
┌─────────────┐  ┌──────────────────────────────┐
│ clinical-   │  │  bedside-telemetry-service   │ <-- Restores trace from Kafka headers
│ chart-serv. │  │  (Crash Cart Pagers)         │     Maintains unbroken audit chain
└─────────────┘  └──────────────────────────────┘
```

### The W3C `traceparent` Standard
Spring Boot 3.4.0 replaces legacy Zipkin B3 (`X-B3-TraceId`) with the vendor-neutral W3C standard:
```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
             │  └──────────────┬───────────────┘ └───────┬────────┘ └── Trace Flags (01 = Sampled)
             │                 │                         └───────────── Child Span ID (16 hex)
             │                 └─────────────────────────────────────── Root Trace ID (32 hex)
             └───────────────────────────────────────────────────────── Version (00)
```

---

## 2. Step-by-Step Implementation Guide

### Step 1: API Gateway Trace Generation & Ingress Filter
Create a `GlobalFilter` in Spring Cloud Gateway with `Ordered.HIGHEST_PRECEDENCE`. It checks for an incoming W3C header; if absent or invalid, it creates a new 32-hex Trace ID and 16-hex Span ID.

**File:** `backend-java/api-gateway/src/main/java/com/hospital/gateway/filter/TracePropagationGlobalFilter.java`
```java
@Component
public class TracePropagationGlobalFilter implements GlobalFilter, Ordered {

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final Pattern W3C_PATTERN = 
            Pattern.compile("^00-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-[0-9a-fA-F]{2}$");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String inbound = request.getHeaders().getFirst(TRACEPARENT_HEADER);

        String traceId;
        String spanId;
        String outgoingTraceparent;

        if (inbound != null && W3C_PATTERN.matcher(inbound).matches()) {
            outgoingTraceparent = inbound;
            traceId = inbound.split("-")[1];
        } else {
            traceId = UUID.randomUUID().toString().replace("-", ""); // 32 hex
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16); // 16 hex
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        }

        // 1. Mutate request headers sent downstream
        ServerHttpRequest mutatedRequest = request.mutate()
                .header(TRACEPARENT_HEADER, outgoingTraceparent)
                .header(CORRELATION_ID_HEADER, traceId)
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(mutatedRequest)
                .build();

        // 2. Attach X-Correlation-ID to client response headers
        mutatedExchange.getResponse().beforeCommit(() -> {
            mutatedExchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, traceId);
            return Mono.empty();
        });

        return chain.filter(mutatedExchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
```

---

### Step 2: Microservice Maven POM & Logback MDC Pattern

In downstream services, import `micrometer-tracing-bridge-otel`:

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <!-- Micrometer Tracing Core & OpenTelemetry Bridge -->
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-tracing-bridge-otel</artifactId>
    </dependency>
    <dependency>
        <groupId>io.opentelemetry</groupId>
        <artifactId>opentelemetry-exporter-otlp</artifactId>
    </dependency>
</dependencies>
```

Configure `src/main/resources/logback-spring.xml` to include `%X{traceId}` and `%X{spanId}`:
```xml
<configuration>
    <property name="HIS_LOG_PATTERN"
              value="%d{yyyy-MM-dd'T'HH:mm:ss.SSSXXX} %-5level [%X{traceId:-NO_TRACE},%X{spanId:-NO_SPAN}] [%t] %logger{36} : %msg%n"/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>${HIS_LOG_PATTERN}</pattern>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

---

### Step 3: Inter-Service HTTP Propagation (`RestClient`)

> **CRITICAL RULE:** Do NOT use `RestClient.create()` or `new RestTemplate()`.  
> Always inject the Spring-managed `RestClient.Builder`. It contains the `ObservationRegistry` that injects `traceparent` automatically.

```java
@Configuration(proxyBeanMethods = false)
public class HttpClientConfig {

    @Bean
    public RestClient clinicalChartRestClient(RestClient.Builder builder) {
        return builder
                .baseUrl("http://clinical-chart-service.his.internal:8082")
                .build();
    }
}
```

When you call:
```java
clinicalChartRestClient.post()
    .uri("/internal/v1/encounters/initiate")
    .body(payload)
    .retrieve()
    .toBodilessEntity();
```
Spring Boot 3.4.0 automatically serializes the active trace into the outbound `traceparent` HTTP header.

---

### Step 4: Asynchronous Kafka Event Propagation

In Spring for Apache Kafka 3.2+ / Spring Boot 3.4.0:

```java
@Configuration
public class KafkaObservationConfig {

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(pf);
        template.setObservationEnabled(true); // Injects traceparent into Kafka RecordHeaders
        return template;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setObservationEnabled(true); // Restores trace in consumer thread
        return factory;
    }
}
```

---

### Step 5: Java 17/21 Async & Virtual Threads (Context Propagation)

`ThreadLocal` MDC is lost inside worker threads or Project Loom Virtual Threads unless explicitly transferred using `io.micrometer:context-propagation`:

```java
@Configuration
@EnableAsync
public class AsyncTracingConfig {

    @Bean(name = "hisTaskExecutor")
    public Executor hisTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(64);
        executor.setThreadNamePrefix("his-async-");
        
        // Transfers MDC & Observation context snapshot to worker thread
        executor.setTaskDecorator(runnable -> ContextSnapshot.captureAll().wrap(runnable));
        executor.initialize();
        return executor;
    }
}
```

---

### Step 6: HIPAA § 164.312(b) Compliance: No Plain PHI in Logs

Never log raw Medical Record Numbers (MRN) or Patient SSNs. Use deterministic salted hashing:

```java
public final class AuditCorrelationHelper {
    private static final String SALT = System.getenv().getOrDefault("HIS_AUDIT_SALT", "k9#xL92@HIS_SECURE_SALT");

    public static String hashPatientIdentifier(String rawMrn) {
        if (rawMrn == null) return "ANONYMOUS";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(SALT.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest(rawMrn.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 16);
        } catch (Exception e) {
            return "HASH_ERROR";
        }
    }
}
```

---

## 3. How to Check & Verify ID Tracing (Verification Playbook)

### Check 1: Ingress Response Headers via cURL
Execute a request against the API Gateway:
```bash
curl -i -X POST http://localhost:8080/api/v1/er/triage-admissions \
  -H "Content-Type: application/json" \
  -d '{"patientMRN":"MRN-902148","ward":"TRAUMA-BAY-3"}'
```

**Verification Criteria:**
1. Response headers must contain:
   ```http
   HTTP/1.1 200 OK
   X-Correlation-ID: 4bf92f3577b34da6a3ce929d0e0e4736
   traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
   ```
2. Verify that `traceId` is exactly 32 hex characters and `spanId` is 16 hex characters.

---

### Check 2: Multi-Service MDC Log Correlation
Query your log aggregator (ELK, Grafana Loki, or Splunk) using the Trace ID:
```
traceId: "4bf92f3577b34da6a3ce929d0e0e4736"
```

**Expected Correlated Stream:**
```text
# 1. API Gateway Log:
2026-11-01T18:40:01.102Z INFO  [his-api-gateway,4bf92f3577b34da6a3ce929d0e0e4736,00f067aa0ba902b7] : [GATEWAY-INGRESS] Path=/api/v1/er/triage-admissions

# 2. Patient Admission Service Log:
2026-11-01T18:40:01.118Z INFO  [patient-admission-service,4bf92f3577b34da6a3ce929d0e0e4736,5a3bc891f6d8924e] : Received admission. Reserved TRAUMA-BAY-3

# 3. Clinical Chart Service Log (called via RestClient):
2026-11-01T18:40:01.138Z INFO  [clinical-chart-service,4bf92f3577b34da6a3ce929d0e0e4736,77c289ff12aa3190] : Chart encounter initialized

# 4. Telemetry Consumer Service Log (consumed from Kafka):
2026-11-01T18:40:01.210Z INFO  [bedside-telemetry-service,4bf92f3577b34da6a3ce929d0e0e4736,b947a1120038cdfe] : Restored trace context from Kafka RecordHeaders. Monitor sync complete.
```
* **Pass:** All 4 services share the exact same `traceId` (`4bf92f3577b34da6a3ce929d0e0e4736`), while each service generates its own unique `spanId`.
* **Fail:** If any service outputs `[NO_TRACE]` or generates a new unrelated Trace ID, check whether `RestClient.Builder` or `setObservationEnabled(true)` was bypassed.

---

### Check 3: Kafka Message RecordHeaders Inspection
Use `kcat` (formerly `kafkacat`) to inspect raw Kafka partition headers:
```bash
kcat -b localhost:9092 -t his.er.events -C -e -f 'Headers: %h\nPayload: %s\n'
```
**Verification Criteria:**
Output must show the byte array header:
```text
Headers: traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-5a3bc891f6d8924e-01
```

---

### Check 4: Automated Spring Boot 3.4 Integration Test
Add an automated verification test that asserts MDC population:

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(OutputCaptureExtension.class)
class DistributedTracingIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void testTraceHeaderPropagationAndMdcLogging(CapturedOutput output) {
        HttpHeaders headers = new HttpHeaders();
        String sampleTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        headers.set("traceparent", sampleTraceparent);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(
                Map.of("patientMRN", "MRN-902148", "ward", "TRAUMA-BAY-3"), 
                headers
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admissions", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        
        // Assert that the trace ID was extracted and printed in the SLF4J MDC log output
        assertThat(output.getAll()).contains("4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
```

---

## 4. Production Debugging Checklist

| Symptom | Root Cause | Fix |
| :--- | :--- | :--- |
| Downstream service shows `[NO_TRACE]` | Calling via `new RestTemplate()` or `RestClient.create()` | Inject `RestClient.Builder` configured with Spring Boot's `ObservationRegistry` |
| Trace breaks at Kafka consumer | `setObservationEnabled(true)` missing | Set `template.setObservationEnabled(true)` on Producer and ContainerFactory |
| `traceId` missing in `@Async` methods | `ThreadLocal` not copied to thread pool | Add `TaskDecorator` with `ContextSnapshot.captureAll().wrap(runnable)` |
| Gateway creates duplicate root spans | Gateway filter order too low | Set `Ordered.HIGHEST_PRECEDENCE` so filter runs before routing |
| Unformatted logs in production | Missing logback pattern | Ensure `[%X{traceId:-NO_TRACE},%X{spanId:-NO_SPAN}]` is present in log pattern |
