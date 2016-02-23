package org.chronopolis.intake.duracloud.model;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.remote.model.History;

import java.util.List;

/**
 * This is totally the same as the BaggingHistory, I wonder if we can combine them
 *
 * Created by shake on 11/19/15.
 */
public class ReplicationHistory extends History {

    private static final String snapshotAction = "SNAPSHOT_REPLICATED";
    private final String snapshotId;
    ReplicationReceipt history;

    public ReplicationHistory(String snapshotId, boolean alternate) {
        this.snapshotId = snapshotId;
        setAlternate(alternate);
    }

    @Override
    public List<ReplicationReceipt> getHistory() {
        // return history;
        // this isn't really used for this class but w.e.
        return ImmutableList.of(history);
    }

    @Override
    public String getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String getSnapshotAction() {
        return snapshotAction;
    }

    public void addReplicationReceipt(ReplicationReceipt receipt) {
        this.history = receipt;
    }

    public void addReplicationReceipt(String name, String node) {
        ReplicationReceipt receipt = new ReplicationReceipt();
        receipt.addBagId(name);
        receipt.setNode(node);
        addReplicationReceipt(receipt);
    }

    public String toString() {
        return history.toString();
    }

}
