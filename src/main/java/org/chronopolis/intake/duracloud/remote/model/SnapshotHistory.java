package org.chronopolis.intake.duracloud.remote.model;

import java.util.List;

/**
 * History returned by the Bridge for a Snapshot
 *
 * Created by shake on 11/11/15.
 */
public class SnapshotHistory {

    private Long totalCount;
    private List<HistoryItem> historyItems;

    public Long getTotalCount() {
        return totalCount;
    }

    public SnapshotHistory setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
        return this;
    }

    public List<HistoryItem> getHistoryItems() {
        return historyItems;
    }

    public SnapshotHistory setHistoryItems(List<HistoryItem> historyItems) {
        this.historyItems = historyItems;
        return this;
    }
}
