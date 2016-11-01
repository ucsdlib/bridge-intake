package org.chronopolis.intake.duracloud.config.props;

/**
 *
 * Created by shake on 10/31/16.
 */
public class Duracloud {

    private String snapshots = "/dc/snapshots";
    private String restores = "/dc/restore";
    private String manifest = "manifest-sha256.txt";
    private String host = "localhost";
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

    /*
    public String getHost() {
        return host;
    }

    public Duracloud setHost(String host) {
        this.host = host;
        return this;
    }
    */

    public Bridge getBridge() {
        return bridge;
    }

    public Duracloud setBridge(Bridge bridge) {
        this.bridge = bridge;
        return this;
    }
}
