package com.hospital.gateway.filter;

import io.micrometer.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.reactive.ServerHttpObservationFilter;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.regex.Pattern;

/**
 * Hospital Information System (HIS) - Enterprise Edge Trace Filter
 *
 * Coordinates OpenTelemetry / Micrometer W3C Trace Context with client correlation headers.
 */
@Component
public class TracePropagationGlobalFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(TracePropagationGlobalFilter.class);

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    // W3C Pattern: version(2)-traceId(32)-spanId(16)-traceFlags(2)
    private static final Pattern W3C_TRACEPARENT_PATTERN =
            Pattern.compile("^00-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-[0-9a-fA-F]{2}$");

    private final Tracer tracer;

    public TracePropagationGlobalFilter(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String inboundTraceparent = request.getHeaders().getFirst(TRACEPARENT_HEADER);

        String traceId = null;
        String spanId = null;

        // 1. First Priority: Extract the active Spring WebFlux Observation on this exchange
        Observation observation = exchange.getAttribute(ServerHttpObservationFilter.CURRENT_OBSERVATION_ATTRIBUTE);
        if (observation != null) {
            TracingObservationHandler.TracingContext tracingContext =
                    observation.getContextView().getOrDefault(TracingObservationHandler.TracingContext.class, null);
            if (tracingContext != null && tracingContext.getSpan() != null) {
                traceId = tracingContext.getSpan().context().traceId();
                spanId = tracingContext.getSpan().context().spanId();
            }
        }

        // 2. Second Priority: Check if Tracer has an active span
        if (traceId == null) {
            Span currentSpan = tracer.currentSpan();
            if (currentSpan != null) {
                traceId = currentSpan.context().traceId();
                spanId = currentSpan.context().spanId();
            }
        }

        // 3. Third Priority: If client sent an explicit W3C header, parse it
        if (traceId == null && inboundTraceparent != null && W3C_TRACEPARENT_PATTERN.matcher(inboundTraceparent).matches()) {
            String[] parts = inboundTraceparent.split("-");
            traceId = parts[1];
            spanId = parts[2];
        }

        // 4. Fallback: Generate valid identifiers if no observation was found
        if (traceId == null) {
            Span newSpan = tracer.nextSpan().name("gateway-ingress").start();
            traceId = newSpan.context().traceId();
            spanId = newSpan.context().spanId();
        }

        String outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        final String finalTraceId = traceId;
        final String finalOutgoingTraceparent = outgoingTraceparent;

        // Mutate downstream headers using set() so Netty and downstream receive identical headers
        ServerHttpRequest mutatedRequest = request.mutate()
                .headers(httpHeaders -> {
                    httpHeaders.set(TRACEPARENT_HEADER, finalOutgoingTraceparent);
                    httpHeaders.set(CORRELATION_ID_HEADER, finalTraceId);
                })
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(mutatedRequest)
                .build();

        // Attach X-Correlation-ID and traceparent to client HTTP response
        mutatedExchange.getResponse().beforeCommit(() -> {
            mutatedExchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, finalTraceId);
            mutatedExchange.getResponse().getHeaders().set(TRACEPARENT_HEADER, finalOutgoingTraceparent);
            return Mono.empty();
        });

        // Populate MDC for reactive thread logging
        org.slf4j.MDC.put("traceId", finalTraceId);
        org.slf4j.MDC.put("spanId", spanId);
        try {
            log.info("[GATEWAY-INGRESS] Path={} Method={} TraceId={} ClientIP={}",
                    request.getPath(),
                    request.getMethod(),
                    finalTraceId,
                    request.getRemoteAddress());
        } finally {
            org.slf4j.MDC.remove("traceId");
            org.slf4j.MDC.remove("spanId");
        }

        return chain.filter(mutatedExchange);
    }

    @Override
    public int getOrder() {
        // Run after ServerHttpObservationFilter has initiated the WebFlux Observation
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }
}
