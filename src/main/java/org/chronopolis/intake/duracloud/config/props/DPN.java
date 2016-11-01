package org.chronopolis.intake.duracloud.config.props;

/**
 *
 * Created by shake on 11/1/16.
 */
public class DPN {

    private String endpoint = "http://localhost:3000/";
    private String username = "chron";
    private String apiKey = "chron-token";

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
