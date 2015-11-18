package org.chronopolis.intake.duracloud.remote.model;

/**
 * An item return by the snapshot history
 *
 * Created by shake on 11/11/15.
 */
public class HistoryItem {

    private String history;
    private Long historyDate;

    public String getHistory() {
        return history;
    }

    public HistoryItem setHistory(String history) {
        this.history = history;
        return this;
    }

    public Long getHistoryDate() {
        return historyDate;
    }

    public HistoryItem setHistoryDate(Long historyDate) {
        this.historyDate = historyDate;
        return this;
    }
}
