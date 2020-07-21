package com.trident.load_balancer;

import org.mitre.dsmiley.httpproxy.ProxyServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

@Configuration
public class TridentProxyServletConfiguration {

    private Cluster cluster;

    public TridentProxyServletConfiguration(Cluster cluster) {
        this.cluster = cluster;
    }

    @Bean
    public List<ServletRegistrationBean> proxyServletRegistrationBean() {
        return cluster
                .getAvailableNodes()
                .stream()
                .map(Node::getHostName)
                .map(constructProxyMapping())
                .collect(toList());
    }

    private Function<String, ServletRegistrationBean> constructProxyMapping() {
        return url -> {
            ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean(new ProxyServlet(), "/*");
            servletRegistrationBean.addInitParameter("targetUri", url);
            return servletRegistrationBean;
        };
    }
}