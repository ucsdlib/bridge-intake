package org.chronopolis.intake.duracloud.model;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.remote.model.History;

import java.util.List;

/**
 * An empty history object so we don't return null
 *
 * Created by shake on 2/25/16.
 */
public class NullHistory extends History {
    @Override
    public List getHistory() {
        return ImmutableList.of();
    }

    @Override
    public String getSnapshotId() {
        return "";
    }

    @Override
    public String getSnapshotAction() {
        return "";
    }
}
