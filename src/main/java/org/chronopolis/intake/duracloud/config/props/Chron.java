package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 *
 * Created by shake on 10/31/16.
 */
@ConfigurationProperties(prefix = "chron")
public class Chron {

    private Ingest ingest;

    /**
     * Name of the local chronopolis user
     */
    private String node;

    /**
     * Location of the bag staging directory
     */
    private String bags = "/staging/bags";

    /**
     * Location of the token staging directory
     */
    private String tokens = "/staging/tokens";

    /**
     * Location of the restoration staging directory
     */
    private String restoration = "/staging/restoration";

    /**
     * Location of the preservation storage
     */
    private String preservation = "/data/preservation";

    /**
     * List of nodes to replicate to
     */
    private List<String> replicatingTo;

    public String getNode() {
        return node;
    }

    public Chron setNode(String node) {
        this.node = node;
        return this;
    }

    public String getBags() {
        return bags;
    }

    public Chron setBags(String bags) {
        this.bags = bags;
        return this;
    }

    public String getTokens() {
        return tokens;
    }

    public Chron setTokens(String tokens) {
        this.tokens = tokens;
        return this;
    }

    public String getRestoration() {
        return restoration;
    }

    public Chron setRestoration(String restoration) {
        this.restoration = restoration;
        return this;
    }

    public String getPreservation() {
        return preservation;
    }

    public Chron setPreservation(String preservation) {
        this.preservation = preservation;
        return this;
    }

    public List<String> getReplicatingTo() {
        return replicatingTo;
    }

    public Chron setReplicatingTo(List<String> replicatingTo) {
        this.replicatingTo = replicatingTo;
        return this;
    }

    public Ingest getIngest() {
        return ingest;
    }

    public Chron setIngest(Ingest ingest) {
        this.ingest = ingest;
        return this;
    }
}
