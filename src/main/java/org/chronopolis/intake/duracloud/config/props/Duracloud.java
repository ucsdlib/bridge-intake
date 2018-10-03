package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shake on 10/31/16.
 */
@ConfigurationProperties("duracloud")
public class Duracloud {

    private List<Bridge> bridge = new ArrayList<>();

    public List<Bridge> getBridge() {
        return bridge;
    }

    public Duracloud setBridge(List<Bridge> bridge) {
        this.bridge = bridge;
        return this;
    }

    public static class Bridge {

        /**
         * Member prefix for Chronopolis deposits
         */
        private String prefix = "";

        /**
         * Name to refer to this Bridge by
         */
        private String name = "bridge-default";

        /**
         * Directory of duracloud snapshots
         */
        private String snapshots = "/dc/snapshots";

        /**
         * Directory of duracloud restores
         */
        private String restores = "/dc/restore";

        /**
         * Default manifest name to use
         */
        private String manifest = "manifest-sha256.txt";

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

        private Push push = Push.CHRONOPOLIS;

        public String getSnapshots() {
            return snapshots;
        }

        public Bridge setSnapshots(String snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        public String getRestores() {
            return restores;
        }

        public Bridge setRestores(String restores) {
            this.restores = restores;
            return this;
        }

        public String getManifest() {
            return manifest;
        }

        public Bridge setManifest(String manifest) {
            this.manifest = manifest;
            return this;
        }

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

        public Push getPush() {
            return push;
        }

        public Bridge setPush(Push push) {
            this.push = push;
            return this;
        }

        public String getPrefix() {
            return prefix;
        }

        public Bridge setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public String getName() {
            return name;
        }

        public Bridge setName(String name) {
            this.name = name;
            return this;
        }
    }

}
