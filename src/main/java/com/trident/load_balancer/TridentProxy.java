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
    private RestTemplate restTemplate;
    private LoadBalancer loadBalancer;

    public TridentProxy(RestTemplate restTemplate, LoadBalancer loadBalancer) {
        this.restTemplate = restTemplate;
        this.loadBalancer = loadBalancer;
    }

    @RequestMapping(value = "/**")
    public ResponseEntity handleRequest(HttpServletRequest request) throws IOException {
        try {
            return forwardRequest(request);
        } catch (final HttpClientErrorException e) {
            return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
        }
    }

    private ResponseEntity forwardRequest(HttpServletRequest request) throws IOException {
        String targetURL = getTargetNodeURL(request);
        String body = resolveRequestBody(request);
        ResponseEntity<Object> exchange = doHTTPRequest(request, targetURL, body);
        return exchange;
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
        String nextAvailableHost = loadBalancer.getNextAvailableHost();
        String requestPath = request.getRequestURI();
        return nextAvailableHost + requestPath;
    }
}
