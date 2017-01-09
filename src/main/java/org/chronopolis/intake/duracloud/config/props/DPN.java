package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * Created by shake on 11/1/16.
 */
@ConfigurationProperties(value = "dpn")
public class DPN {

    /**
     * The endpoint of the local dpn registry
     */
    private String endpoint = "http://localhost:3000/";

    /**
     * The namespace of the local dpn user
     */
    private String username = "chron";

    /**
     * The api key for the local dpn user
     */
    private String apiKey = "replace-me";

    public String getEndpoint() {
        return endpoint;
    }

    public DPN setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DPN setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getApiKey() {
        return apiKey;
    }

    public DPN setApiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }
}
