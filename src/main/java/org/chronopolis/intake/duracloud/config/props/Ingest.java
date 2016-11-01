package org.chronopolis.intake.duracloud.config.props;

/**
 *
 * Created by shake on 11/1/16.
 */
public class Ingest {

    private String username = "admin";
    private String password = "admin";
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
