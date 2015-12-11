package org.chronopolis.intake.duracloud.model;

/**
 *
 * Created by shake on 11/19/15.
 */
public class ReplicationReceipt {

    private String name;
    private String node;

    public String getNode() {
        return node;
    }

    public ReplicationReceipt setNode(String node) {
        this.node = node;
        return this;
    }

    public String getName() {
        return name;
    }

    public ReplicationReceipt setName(String name) {
        this.name = name;
        return this;
    }

    public String toString() {
        return name + " -> " + node;
    }
}
