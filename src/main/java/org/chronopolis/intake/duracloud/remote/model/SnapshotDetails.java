package org.chronopolis.intake.duracloud.remote.model;

/**
 * Snapshot details from bridge/snapshot/{snapshotId}
 *
 * Created by shake on 7/20/15.
 */
public class SnapshotDetails {

    private String snapshotId;
    private String snapshotDate;
    private SnapshotStatus status;
    private String sourceHost;
    private String sourceSpaceId;
    private String sourceStoreId;
    private String description;
    private String contentItemCount;
    private String totalSizeInBytes;
    private String memberId;

    public String getSnapshotId() {
        return snapshotId;
    }

    public SnapshotDetails setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public String getSnapshotDate() {
        return snapshotDate;
    }

    public SnapshotDetails setSnapshotDate(String snapshotDate) {
        this.snapshotDate = snapshotDate;
        return this;
    }

    public SnapshotStatus getStatus() {
        return status;
    }

    public SnapshotDetails setStatus(SnapshotStatus status) {
        this.status = status;
        return this;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public SnapshotDetails setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
        return this;
    }

    public String getSourceSpaceId() {
        return sourceSpaceId;
    }

    public SnapshotDetails setSourceSpaceId(String sourceSpaceId) {
        this.sourceSpaceId = sourceSpaceId;
        return this;
    }

    public String getSourceStoreId() {
        return sourceStoreId;
    }

    public SnapshotDetails setSourceStoreId(String sourceStoreId) {
        this.sourceStoreId = sourceStoreId;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SnapshotDetails setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getContentItemCount() {
        return contentItemCount;
    }

    public SnapshotDetails setContentItemCount(String contentItemCount) {
        this.contentItemCount = contentItemCount;
        return this;
    }

    public String getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public SnapshotDetails setTotalSizeInBytes(String totalSizeInBytes) {
        this.totalSizeInBytes = totalSizeInBytes;
        return this;
    }

    public String getMemberId() {
        return memberId;
    }

    public SnapshotDetails setMemberId(String memberId) {
        this.memberId = memberId;
        return this;
    }
}
