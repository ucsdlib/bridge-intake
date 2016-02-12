package org.chronopolis.intake.duracloud.scheduled;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.HistoryItem;
import org.chronopolis.intake.duracloud.remote.model.Snapshot;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.intake.duracloud.remote.model.SnapshotHistory;
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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

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
    public void findSnapshots() throws IOException {
        log.trace("Polling for snapshots...");
        Snapshots snapshots;
        Call<Snapshots> snapshotCall = bridge.getSnapshots(null, SnapshotStatus.WAITING_FOR_DPN);
        Response<Snapshots> response = snapshotCall.execute();
        if (response.isSuccess()) {
            snapshots = response.body();
        } else {
            log.error("Unable to query Bridge API: {}", response.message());
            log.error("{}", response.errorBody().string());
            return;
        }

        for (Snapshot snapshot : snapshots.getSnapshots()) {
            String snapshotId = snapshot.getSnapshotId();
            SnapshotDetails details;
            SnapshotHistory history;

            Call<SnapshotDetails> detailsCall = bridge.getSnapshotDetails(snapshotId);
            Call<SnapshotHistory> historyCall = bridge.getSnapshotHistory(snapshotId, null);

            Response<SnapshotDetails> detailsResponse = detailsCall.execute();
            Response<SnapshotHistory> historyResponse = historyCall.execute();
            details = detailsResponse.body();
            history = historyResponse.body();

            if (history.getTotalCount() > 0) {
                // try to deserialize the history
                Gson gson = new GsonBuilder().create();
                List<BagReceipt> validReceipts = new ArrayList<>();
                for (HistoryItem historyItem : history.getHistoryItems()) {
                    log.info(historyItem.getHistory());
                    try {
                        Type type = new TypeToken<List<BagReceipt>>() {
                        }.getType();
                        List<BagReceipt> bd = gson.fromJson(historyItem.getHistory(), type);
                        for (BagReceipt receipt : bd) {
                            log.info("{} ? {} ", receipt.isInitialized(), (receipt.isInitialized() ? receipt.getName() : "null"));
                            if (receipt.isInitialized()) {
                                validReceipts.add(receipt);
                            }

                        }
                    } catch (Exception e) {
                        log.warn("Error deserializing some of the history", e);
                    }
                }
                manager.startReplicationTasklet(details, validReceipts, settings);
            } else {
                // bag
                log.info("Bagging snapshot ", snapshotId);
                manager.startSnapshotTasklet(details);
            }

        }
    }

    // I don't think we need this - depends on when we close snapshots
    public void updateSnapshots() {
    }

}
