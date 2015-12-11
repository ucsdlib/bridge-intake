package org.chronopolis.intake.duracloud.remote.model;

/**
 * Snapshot item from bridge/snapshot
 *
 * Created by shake on 7/20/15.
 */
public class Snapshot {

    private String snapshotId;
    private String description;
    private SnapshotStatus status;

    public String getSnapshotId() {
        return snapshotId;
    }

    public Snapshot setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Snapshot setDescription(String description) {
        this.description = description;
        return this;
    }

    public SnapshotStatus getStatus() {
        return status;
    }

    public Snapshot setStatus(SnapshotStatus status) {
        this.status = status;
        return this;
    }
}
