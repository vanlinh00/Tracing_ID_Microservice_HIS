package com.hospital.gateway.filter;

import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class TracePropagationGlobalFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(TracePropagationGlobalFilter.class);

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    public static final String TRACE_ID_HEADER = "X-Trace-Id";

    private static final Pattern W3C_PATTERN =
            Pattern.compile("^00-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})-([0-9a-fA-F]{2})$");

    private final Tracer tracer;

    @Autowired(required = false)
    public TracePropagationGlobalFilter(Tracer tracer) {
        this.tracer = tracer;
    }

    public TracePropagationGlobalFilter() {
        this.tracer = null;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String inbound = request.getHeaders().getFirst(TRACEPARENT_HEADER);
        String inboundCorrelationId = request.getHeaders().getFirst(CORRELATION_ID_HEADER);
        String inboundTraceId = request.getHeaders().getFirst(TRACE_ID_HEADER);

        String traceId;
        String spanId;
        String outgoingTraceparent;

        if (inbound != null && W3C_PATTERN.matcher(inbound).matches()) {
            Matcher matcher = W3C_PATTERN.matcher(inbound);
            matcher.find();
            traceId = matcher.group(1);
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        } else if (tracer != null && tracer.currentSpan() != null) {
            traceId = tracer.currentSpan().context().traceId();
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        } else if (inboundCorrelationId != null && !inboundCorrelationId.isBlank()) {
            traceId = inboundCorrelationId.trim();
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        } else if (inboundTraceId != null && !inboundTraceId.isBlank()) {
            traceId = inboundTraceId.trim();
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        } else {
            traceId = UUID.randomUUID().toString().replace("-", ""); // 32 hex
            spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16); // 16 hex
            outgoingTraceparent = String.format("00-%s-%s-01", traceId, spanId);
        }

        log.info("[GATEWAY-INGRESS] Path={} Method={} TraceId={} ClientIP={}",
                request.getPath(), request.getMethod(), traceId, request.getRemoteAddress());

        // 1. Mutate request headers sent downstream
        ServerHttpRequest mutatedRequest = request.mutate()
                .header(TRACEPARENT_HEADER, outgoingTraceparent)
                .header(CORRELATION_ID_HEADER, traceId)
                .header(TRACE_ID_HEADER, traceId)
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(mutatedRequest)
                .build();

        // 2. Attach X-Correlation-ID, X-Trace-Id, and traceparent to client response headers
        mutatedExchange.getResponse().beforeCommit(() -> {
            mutatedExchange.getResponse().getHeaders().set(CORRELATION_ID_HEADER, traceId);
            mutatedExchange.getResponse().getHeaders().set(TRACE_ID_HEADER, traceId);
            mutatedExchange.getResponse().getHeaders().set(TRACEPARENT_HEADER, outgoingTraceparent);
            return Mono.empty();
        });

        return chain.filter(mutatedExchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
