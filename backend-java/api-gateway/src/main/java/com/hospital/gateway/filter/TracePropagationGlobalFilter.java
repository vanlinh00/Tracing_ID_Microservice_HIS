package com.hospital.gateway.filter;

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
 * Responsibilities:
 * 1. Inspect incoming requests for W3C 'traceparent' header.
 * 2. If missing or invalid, generate RFC-compliant 32-character hex trace ID and 16-character span ID.
 * 3. Mutate the request passing the canonical W3C traceparent and X-Correlation-ID downstream.
 * 4. Add X-Correlation-ID to client response headers before committing.
 */
@Component
public class TracePropagationGlobalFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(TracePropagationGlobalFilter.class);

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    // W3C Pattern: version(2)-traceId(32)-spanId(16)-traceFlags(2)
    private static final Pattern W3C_TRACEPARENT_PATTERN =
            Pattern.compile("^00-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-[0-9a-fA-F]{2}$");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String inboundTraceparent = request.getHeaders().getFirst(TRACEPARENT_HEADER);

        String traceId;
        String spanId;
        String outgoingTraceparent;

        if (inboundTraceparent != null && W3C_TRACEPARENT_PATTERN.matcher(inboundTraceparent).matches()) {
            String[] parts = inboundTraceparent.split("-");
            traceId = parts[1];
            spanId = parts[2];
            outgoingTraceparent = inboundTraceparent;
        } else {
            // Generate clean hex characters without hyphens
            traceId = UUID.randomUUID().toString().replace("-", "");
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        }

        // 1. Mutate downstream request headers
        ServerHttpRequest mutatedRequest = request.mutate()
                .header(TRACEPARENT_HEADER, outgoingTraceparent)
                .header(CORRELATION_ID_HEADER, traceId)
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(mutatedRequest)
                .build();

        // 2. Attach X-Correlation-ID to response headers for the client / EHR terminal
        mutatedExchange.getResponse().beforeCommit(() -> {
            mutatedExchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, traceId);
            return Mono.empty();
        });

        // 3. Populate MDC for reactive thread logging so [traceId, spanId] is not NO_TRACE
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
        // Run before Spring Cloud Gateway routing filters
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
