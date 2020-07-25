package com.trident.load_balancer;

import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.Charset;

@RestController
public class TridentProxy {
    private final RestTemplate restTemplate;

    private final LoadBalancer loadBalancer;

    private final RequestThrottler requestThrottler;

    public TridentProxy(RestTemplate restTemplate, LoadBalancer loadBalancer, RequestThrottler requestThrottler) {
        this.restTemplate = restTemplate;
        this.loadBalancer = loadBalancer;
        this.requestThrottler = requestThrottler;
    }

    @RequestMapping(value = "/**")
    public ResponseEntity handleRequest(HttpServletRequest request) throws IOException {
        if (!requestThrottler.canProceed()) {
            return ResponseEntity.unprocessableEntity().build();
        }
        try {
            return forwardRequest(request);
        } catch (final HttpClientErrorException e) {
            return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
        }
    }

    private ResponseEntity forwardRequest(HttpServletRequest request) throws IOException {
        String targetURL = getTargetNodeURL(request);
        String body = resolveRequestBody(request);
        return doHTTPRequest(request, targetURL, body);
    }

    private ResponseEntity<Object> doHTTPRequest(HttpServletRequest request, String targetURL, String body) {
        return restTemplate.exchange(targetURL,
                HttpMethod.valueOf(request.getMethod()),
                new HttpEntity<>(body),
                Object.class,
                request.getParameterMap());
    }

    private String resolveRequestBody(HttpServletRequest request) throws IOException {
        return IOUtils.toString(request.getInputStream(), Charset.forName(request.getCharacterEncoding()));
    }

    private String getTargetNodeURL(HttpServletRequest request) {
        String nextAvailableHostName = loadBalancer.getNextAvailableHost().getHostName();
        String requestPath = request.getRequestURI();
        return nextAvailableHostName + requestPath;
    }
}
