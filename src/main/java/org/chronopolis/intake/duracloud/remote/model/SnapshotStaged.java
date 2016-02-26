package org.chronopolis.intake.duracloud.remote.model;

/**
 *
 * Created by shake on 2/26/16.
 */
public class SnapshotStaged {

    private String snapshotId;

    public SnapshotStaged() {
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public SnapshotStaged setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public String getSnapshotAction() {
        return "SNAPSHOT_STAGED";
    }
}
