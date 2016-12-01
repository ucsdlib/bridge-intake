package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * Created by shake on 10/31/16.
 */
@ConfigurationProperties(value = "duracloud.bridge")
public class Bridge {

    /**
     * Username when connecting to the bridge API
     */
    private String username = "bridge";

    /**
     * Password when connecting to the bridge API
     */
    private String password = "replace-me";

    /**
     * Endpoint of the bridge API
     */
    private String endpoint = "localhost:8000";

    public String getUsername() {
        return username;
    }

    public Bridge setUsername(String user) {
        this.username = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Bridge setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Bridge setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }
}
