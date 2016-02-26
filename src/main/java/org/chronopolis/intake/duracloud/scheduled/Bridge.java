package org.chronopolis.intake.duracloud.scheduled;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.model.BaggingHistoryDeserializer;
import org.chronopolis.intake.duracloud.model.HistoryDeserializer;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistoryItem;
import org.chronopolis.intake.duracloud.remote.model.Snapshot;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.intake.duracloud.remote.model.SnapshotHistory;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStaged;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStagedDeserializer;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStatus;
import org.chronopolis.intake.duracloud.remote.model.Snapshots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

/**
 * Define a scheduled task which polls the Bridge server for snapshots
 * <p/>
 * <p/>
 * Created by shake on 7/27/15.
 */
@Component
@EnableScheduling
public class Bridge {

    private final Logger log = LoggerFactory.getLogger(Bridge.class);

    @Autowired
    BridgeAPI bridge;

    @Autowired
    SnapshotJobManager manager;

    @Autowired
    IntakeSettings settings;

    @Scheduled(cron = "0 * * * * *")
    public void findSnapshots() {
        // TODO: Use enqueue for calls instead of execute, should alleviate some of the try/catch madness
        log.trace("Polling for snapshots...");
        Snapshots snapshots;
        Call<Snapshots> snapshotCall = bridge.getSnapshots(null, SnapshotStatus.WAITING_FOR_DPN);
        Response<Snapshots> response = null;
        try {
            response = snapshotCall.execute();
        } catch (IOException e) {
            log.error("Unable to query Bridge API:", e);
            return;
        }

        if (response != null && response.isSuccess()) {
            snapshots = response.body();
        } else {
            String message = response != null ? response.message() : "";
            log.error("Error in query to bridge api: Bridge API {}", message);
            return;
        }

        for (Snapshot snapshot : snapshots.getSnapshots()) {
            String snapshotId = snapshot.getSnapshotId();
            SnapshotDetails details;
            SnapshotHistory history;

            Call<SnapshotDetails> detailsCall = bridge.getSnapshotDetails(snapshotId);
            Call<SnapshotHistory> historyCall = bridge.getSnapshotHistory(snapshotId, null);

            Response<SnapshotDetails> detailsResponse = null;
            Response<SnapshotHistory> historyResponse = null;
            try {
                detailsResponse = detailsCall.execute();
                historyResponse = historyCall.execute();
            } catch (IOException e) {
                log.error("Error getting History for snapshot {}", snapshotId, e);
                continue;
            }

            details = detailsResponse != null ? detailsResponse.body() : null;
            history = historyResponse != null ? historyResponse.body() : null;

            if (history != null && history.getTotalCount() > 0) {
                // try to deserialize the history
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(History.class, new HistoryDeserializer())
                        .registerTypeAdapter(BaggingHistory.class, new BaggingHistoryDeserializer())
                        .registerTypeAdapter(SnapshotStaged.class, new SnapshotStagedDeserializer())
                        .disableHtmlEscaping()
                        .create();

                // The latest history item should tell us what step we're on, and of those we
                // only really care about SNAPSHOT_STAGED and SNAPSHOT_BAGGED
                // If we're at STAGED, then the snapshot is ready to be bagged
                // If we're at BAGGED, then the snapshot needs to be replicated/closed
                HistoryItem item = history.getHistoryItems().get(0);
                log.debug("Processing line {}", item.getHistory());
                History fromJson = gson.fromJson(item.getHistory(), History.class);
                if (fromJson instanceof SnapshotStaged) {
                    log.info("Bagging snapshot {}", snapshotId);
                    manager.startSnapshotTasklet(details);
                } else if (fromJson instanceof BaggingHistory) {
                    BaggingHistory bHistory = (BaggingHistory) fromJson;
                    manager.startReplicationTasklet(details, bHistory.getHistory(), settings);
                }

                /*
                List<BagReceipt> validReceipts = new ArrayList<>();
                for (HistoryItem historyItem : history.getHistoryItems()) {
                    log.info(historyItem.getHistory());

                    History fromHistory = gson.fromJson(historyItem.getHistory(), History.class);
                    if (fromHistory instanceof BaggingHistory) {
                        BaggingHistory bHistory = (BaggingHistory) fromHistory;
                        validReceipts.addAll(bHistory.getHistory());
                    }
                }

                */
            } else {
                log.warn("Snapshot {} has no history, ignoring", snapshotId);
            }

        }
    }

}
