package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * Created by shake on 11/1/16.
 */
@ConfigurationProperties(value = "chron.ingest")
public class Ingest {

    /**
     * Username when connecting to the chronopolis ingest http server
     */
    private String username = "admin";

    /**
     * Password when connecting to the chronopolis ingest http server
     */
    private String password = "replace-me";

    /**
     * Endpoint for the chronopolis ingest server
     */
    private String endpoint = "http://localhost:8080/";

    public String getUsername() {
        return username;
    }

    public Ingest setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Ingest setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Ingest setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }
}
