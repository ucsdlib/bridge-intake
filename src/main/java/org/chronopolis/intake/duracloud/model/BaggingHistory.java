package org.chronopolis.intake.duracloud.model;

import org.chronopolis.intake.duracloud.remote.model.History;

import java.util.ArrayList;
import java.util.List;

/**
 * History item representing a completed bagging of an object
 *
 * Created by shake on 11/12/15.
 */
public class BaggingHistory extends History {

    private static final String snapshotAction = "SNAPSHOT_BAGGED";

    private final String snapshotId;
    List<BagReceipt> history = new ArrayList<>();

    public BaggingHistory(String snapshotId, boolean alternate) {
        this.snapshotId = snapshotId;
        setAlternate(alternate);
    }

    @Override
    public List<BagReceipt> getHistory() {
        return history;
    }

    @Override
    public String getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String getSnapshotAction() {
        return snapshotAction;
    }

    public void addBaggingData(BagReceipt data) {
        history.add(data);
    }

    public void addBaggingData(String name, String receipt) {
        BagReceipt data = new BagReceipt();
        data.setName(name);
        data.setReceipt(receipt);
        history.add(data);
    }

}
