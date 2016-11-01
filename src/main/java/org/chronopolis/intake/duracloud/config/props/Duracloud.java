package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * Created by shake on 10/31/16.
 */
@ConfigurationProperties(value = "duracloud")
public class Duracloud {

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

    private Bridge bridge;

    public String getSnapshots() {
        return snapshots;
    }

    public Duracloud setSnapshots(String snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    public String getRestores() {
        return restores;
    }

    public Duracloud setRestores(String restores) {
        this.restores = restores;
        return this;
    }

    public String getManifest() {
        return manifest;
    }

    public Duracloud setManifest(String manifest) {
        this.manifest = manifest;
        return this;
    }

    public Bridge getBridge() {
        return bridge;
    }

    public Duracloud setBridge(Bridge bridge) {
        this.bridge = bridge;
        return this;
    }
}
