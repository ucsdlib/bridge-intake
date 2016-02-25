package org.chronopolis.intake.duracloud.remote.model;

import java.util.List;

/**
 * Abstract class so we can upload multiple types of history to the bridge
 *
 * Created by shake on 11/11/15.
 */
public abstract class History {

    private Boolean alternate;

    public abstract List getHistory();
    public abstract String getSnapshotId();
    public abstract String getSnapshotAction();

    public Boolean getAlternate() {
        return alternate;
    }

    public History setAlternate(Boolean alternate) {
        this.alternate = alternate;
        return this;
    }
}
