package org.chronopolis.intake.duracloud.model;

import com.google.common.base.Strings;

/**
 * Class to encapsulate some of the data we need when making bags
 *
 * Created by shake on 7/30/15.
 */
public class BagData {

    private final String prefix;
    private String snapshotId;
    private String name;
    private String depositor;
    private String member;

    public BagData(String prefix) {
        this.prefix = prefix;
    }

    public String snapshotId() {
        return snapshotId;
    }

    public BagData setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public String name() {
        return name;
    }

    public BagData setName(String name) {
        this.name = name;
        return this;
    }

    public String depositor() {
        return depositor;
    }

    public BagData setDepositor(String depositor) {
        this.depositor = Strings.isNullOrEmpty(prefix)
                ? depositor
                : prefix + depositor;
        return this;
    }

    public String member() {
        return member;
    }

    public BagData setMember(String member) {
        this.member = member;
        return this;
    }
}
