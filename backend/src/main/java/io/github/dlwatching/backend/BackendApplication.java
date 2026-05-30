package io.github.dlwatching.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the DL-Watching Backend service.
 *
 * <p>Spring Boot application with embedded gRPC server for receiving
 * Virtual Thread monitoring data from Java Agents.
 */
@SpringBootApplication
public class BackendApplication {
    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
