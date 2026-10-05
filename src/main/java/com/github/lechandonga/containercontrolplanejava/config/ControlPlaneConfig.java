package com.github.lechandonga.containercontrolplanejava.config;

import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ControlPlaneProperties.class)
public class ControlPlaneConfig {

    @Bean
    public ClockService clockService() {
        return new ClockService();
    }

    @Bean
    public NodeRegistry nodeRegistry(ClockService clockService,
                                     ControlPlaneProperties properties) {
        NodeRegistry registry = new NodeRegistry(clockService);
        registry.setHeartbeatTimeoutSeconds(properties.heartbeatTimeoutSeconds());
        return registry;
    }
}
