package org.chronopolis.intake.duracloud.model;

/**
 * A snapshot request from Duracloud
 *
 * Created by shake on 8/1/14.
 */
public class DuracloudRequest {
    private String snapshotID;
    private String collectionName;
    private String depositor;

    public String getSnapshotID() {
        return snapshotID;
    }

    public void setSnapshotID(final String snapshotID) {
        this.snapshotID = snapshotID;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(final String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDepositor() {
        return depositor;
    }

    public void setDepositor(final String depositor) {
        this.depositor = depositor;
    }
}
