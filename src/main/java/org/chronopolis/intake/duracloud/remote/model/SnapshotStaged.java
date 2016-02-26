package org.chronopolis.intake.duracloud.remote.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 * Created by shake on 2/26/16.
 */
public class SnapshotStaged extends History {

    private String snapshotId;

    public SnapshotStaged() {
    }

    @Override
    public List getHistory() {
        return ImmutableList.of();
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
