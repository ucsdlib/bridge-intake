package org.chronopolis.intake.duracloud.model;

import org.chronopolis.intake.duracloud.remote.model.History;

import java.util.ArrayList;
import java.util.List;

/**
 * This is totally the same as the BaggingHistory, I wonder if we can combine them
 *
 * Created by shake on 11/19/15.
 */
public class ReplicationHistory extends History {

    List<ReplicationReceipt> history = new ArrayList<>();

    public ReplicationHistory(boolean alternate) {
        setAlternate(alternate);
    }

    @Override
    public List<ReplicationReceipt> getHistory() {
        return history;
    }

    @Override
    public String getSnapshotId() {
        return null;
    }

    @Override
    public String getSnapshotAction() {
        return null;
    }

    public void addReplicationReceipt(ReplicationReceipt receipt) {
        history.add(receipt);
    }

    public void addReplicationReceipt(String name, String node) {
        ReplicationReceipt receipt = new ReplicationReceipt();
        receipt.setName(name);
        receipt.setNode(node);
        addReplicationReceipt(receipt);
    }

    public String toString() {
        return history.toString();
    }

}
