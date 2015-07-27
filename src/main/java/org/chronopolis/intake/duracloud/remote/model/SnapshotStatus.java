package org.chronopolis.intake.duracloud.remote.model;

/**
 * Various status values a snapshot can have
 * From: https://wiki.duraspace.org/display/CHRONO/Bridge+App+REST+API
 *
 * Created by shake on 7/27/15.
 */
public enum SnapshotStatus {
    INITIALIZED,
    TRANSFERRING_FROM_DURACLOUD,
    WAITING_FOR_DPN,
    CLEANING_UP,
    SNAPSHOT_COMPLETE,
    FAILED_TO_TRANSFER_FROM_DURACLOUD
}
