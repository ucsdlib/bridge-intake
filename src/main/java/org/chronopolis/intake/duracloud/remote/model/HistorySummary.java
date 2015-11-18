package org.chronopolis.intake.duracloud.remote.model;

/**
 * Summary returned by the bridge after adding a history event
 *
 * Created by shake on 11/11/15.
 */
public class HistorySummary {

    Snapshot snapshot;
    String history;

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public HistorySummary setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public String getHistory() {
        return history;
    }

    public HistorySummary setHistory(String history) {
        this.history = history;
        return this;
    }
}
