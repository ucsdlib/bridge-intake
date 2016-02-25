package org.chronopolis.intake.duracloud.model;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created by shake on 11/19/15.
 */
public class ReplicationReceipt {

    private List<String> bagIds = new ArrayList<>();
    private String node;

    public String getNode() {
        return node;
    }

    public ReplicationReceipt setNode(String node) {
        this.node = node;
        return this;
    }

    public List<String> getBagIds() {
        return bagIds;
    }

    public ReplicationReceipt addBagId(String bagId) {
        this.bagIds.add(bagId);
        return this;
    }

    public String toString() {
        return bagIds + " -> " + node;
    }
}
