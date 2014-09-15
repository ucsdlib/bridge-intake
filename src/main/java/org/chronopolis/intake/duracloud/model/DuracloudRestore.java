package org.chronopolis.intake.duracloud.model;

/**
 * Created by shake on 9/15/14.
 */
public class DuracloudRestore {

    private String snapshotID;
    private String location;

    public DuracloudRestore() {

    }

    public DuracloudRestore(final String snapshotID,
                            final String location) {
        this.snapshotID = snapshotID;
        this.location = location;
    }

    public String getSnapshotID() {
        return snapshotID;
    }

    public void setSnapshotID(final String snapshotID) {
        this.snapshotID = snapshotID;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }
}
