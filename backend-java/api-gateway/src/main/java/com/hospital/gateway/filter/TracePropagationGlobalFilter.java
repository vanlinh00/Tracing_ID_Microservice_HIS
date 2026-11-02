package com.hospital.gateway.filter;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;
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

        String traceId;
        String spanId;
        String outgoingTraceparent;

        // 1. Synchronize with Micrometer Tracer's active span if already initiated by ObservationWebFilter
        Span currentSpan = tracer.currentSpan();

        if (currentSpan != null) {
            traceId = currentSpan.context().traceId();
            spanId = currentSpan.context().spanId();
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        } else if (inboundTraceparent != null && W3C_TRACEPARENT_PATTERN.matcher(inboundTraceparent).matches()) {
            String[] parts = inboundTraceparent.split("-");
            traceId = parts[1];
            spanId = parts[2];
            outgoingTraceparent = inboundTraceparent;
        } else {
            // Generate a real span via Micrometer Tracer
            Span newSpan = tracer.nextSpan().name("gateway-ingress").start();
            traceId = newSpan.context().traceId();
            spanId = newSpan.context().spanId();
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        }

        final String finalTraceId = traceId;
        final String finalOutgoingTraceparent = outgoingTraceparent;

        // 2. Overwrite headers using set() instead of add() so no duplicate headers are sent
        ServerHttpRequest mutatedRequest = request.mutate()
                .headers(httpHeaders -> {
                    httpHeaders.set(TRACEPARENT_HEADER, finalOutgoingTraceparent);
                    httpHeaders.set(CORRELATION_ID_HEADER, finalTraceId);
                })
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(mutatedRequest)
                .build();

        // 3. Attach headers to downstream HTTP response
        mutatedExchange.getResponse().beforeCommit(() -> {
            mutatedExchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, finalTraceId);
            mutatedExchange.getResponse().getHeaders().set(TRACEPARENT_HEADER, finalOutgoingTraceparent);
            return Mono.empty();
        });

        // 4. Populate MDC for reactive thread logging
        org.slf4j.MDC.put("traceId", traceId);
        org.slf4j.MDC.put("spanId", spanId);
        try {
            log.info("[GATEWAY-INGRESS] Path={} Method={} TraceId={} ClientIP={}",
                    request.getPath(),
                    request.getMethod(),
                    traceId,
                    request.getRemoteAddress());
        } finally {
            org.slf4j.MDC.remove("traceId");
            org.slf4j.MDC.remove("spanId");
        }

        return chain.filter(mutatedExchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
