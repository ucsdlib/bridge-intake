package org.chronopolis.intake.duracloud.remote.model;

import java.util.List;

/**
 * Hold a response of multiple snapshots
 *
 * Created by shake on 10/23/15.
 */
public class Snapshots {

    private List<Snapshot> snapshots;

    public List<Snapshot> getSnapshots() {
        return snapshots;
    }

    public Snapshots setSnapshots(List<Snapshot> snapshots) {
        this.snapshots = snapshots;
        return this;
    }
}
