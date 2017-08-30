package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 *
 * @author shake
 */
@ConfigurationProperties(prefix = "chron")
public class Chron {

    private Ingest ingest;

    /**
     * Name of the local chronopolis user
     */
    private String node;

    /**
     * Prefix to use when pushing to chronopolis
     */
    private String prefix = "";

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

    public String getPrefix() {
        return prefix;
    }

    public Chron setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
}
