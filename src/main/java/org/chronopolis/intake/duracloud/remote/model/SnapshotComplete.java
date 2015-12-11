package org.chronopolis.intake.duracloud.remote.model;

/**
 * Response for a snapshot complete call
 *
 * Created by shake on 7/27/15.
 */
public class SnapshotComplete {

    SnapshotStatus status;
    String details;

    public SnapshotComplete() {
    }

    public String getDetails() {
        return details;
    }

    public SnapshotComplete setDetails(String details) {
        this.details = details;
        return this;
    }

    public SnapshotStatus getStatus() {
        return status;
    }

    public SnapshotComplete setStatus(SnapshotStatus status) {
        this.status = status;
        return this;
    }
}
