package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.HttpUrl;
import org.chronopolis.intake.duracloud.DataCollector;
import org.chronopolis.intake.duracloud.PropertiesDataCollector;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.model.BagData;
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
import java.util.HashMap;
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

    private final SnapshotJobManager manager;
    private final List<BridgeContext> contexts;

    @Autowired
    public Bridge(SnapshotJobManager manager, List<BridgeContext> contexts) {
        this.manager = manager;
        this.contexts = contexts;
    }

    @Scheduled(cron = "${bridge.poll:0 0 0 * * *}")
    public void findSnapshots() {
        log.trace("Polling for snapshots...");
        contexts.forEach(this::checkBridge);
    }

    /**
     * Query a bridge for ongoing snapshots
     *
     * @param bridgeContext the {@link BridgeContext} to retrieve the {@link BridgeAPI} to query
     */
    private void checkBridge(BridgeContext bridgeContext) {
        log.info("[{}] Querying for snapshots", bridgeContext.getName());
        Response<Snapshots> response;

        Logger contextLogger = bridgeContext.getLogger();
        BridgeAPI bridge = bridgeContext.getApi();

        Call<Snapshots> snapshotCall = bridge.getSnapshots(null, SnapshotStatus.REPLICATING_TO_STORAGE);
        // something like this for logging... basically derive the bridge fqdn from this
        HttpUrl requestUrl = snapshotCall.request().url();
        try {
            response = snapshotCall.execute();
        } catch (IOException e) {
            contextLogger.warn("[{}] Unable to query Bridge API", requestUrl, e);
            return;
        }

        DataCollector collector = new PropertiesDataCollector(bridgeContext);
        if (response != null && response.isSuccessful()) {
            response.body()
                    .getSnapshots()
                    .forEach(snapshot -> processSnapshot(bridgeContext, collector, snapshot));
        } else {
            String message = response != null ? response.message() : "";
            contextLogger.warn("[{}] Error in query to bridge api: Bridge API {}",
                    requestUrl, message);
        }
    }

    /**
     * Query a Bridge for history and detail information about a {@link Snapshot} and forward this
     * information so that it can continue to be processed. Depending on the {@link SnapshotHistory}
     * retrieved a Snapshot will either be bagged (if {@link SnapshotStaged}) or processed for
     * ingestion (if {@link BaggingHistory}).
     *
     * @param bridgeContext the {@link BridgeContext} telling us which {@link BridgeAPI} we are
     *                      operating on
     * @param collector     the {@link DataCollector} to collect additional information
     * @param snapshot      the {@link Snapshot} being processed
     */
    private void processSnapshot(BridgeContext bridgeContext,
                                 DataCollector collector,
                                 Snapshot snapshot) {
        Logger contextLogger = bridgeContext.getLogger();
        BridgeAPI bridge = bridgeContext.getApi();
        String snapshotId = snapshot.getSnapshotId();

        BagData data;
        SnapshotDetails details;
        List<HistoryItem> history;
        Response<SnapshotDetails> detailsResponse;
        Response<SnapshotHistory> historyResponse;
        Call<SnapshotDetails> detailsCall = bridge.getSnapshotDetails(snapshotId);
        Call<SnapshotHistory> historyCall = bridge.getSnapshotHistory(snapshotId, new HashMap<>());

        try {
            detailsResponse = detailsCall.execute();
            historyResponse = historyCall.execute();
            data = collector.collectBagData(snapshotId);
        } catch (IOException e) {
            contextLogger.error("Error getting information for snapshot {}", snapshotId, e);
            return;
        }

        details = detailsResponse != null && detailsResponse.body() != null
                ? detailsResponse.body()
                : null;
        history = historyResponse != null && historyResponse.body() != null
                ? historyResponse.body().getHistoryItems()
                : ImmutableList.of();

        if (details != null && !history.isEmpty() && data != null) {
            // todo: create this from the initial query... no need to make a new one for each bridge
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
            HistoryItem item = history.get(0);
            History fromJson = gson.fromJson(item.getHistory(), History.class);
            if (fromJson instanceof SnapshotStaged) {
                manager.bagSnapshot(data, bridgeContext);
            } else if (fromJson instanceof BaggingHistory) {
                BaggingHistory baggingHistory = (BaggingHistory) fromJson;
                manager.startReplicationTasklet(
                        data,
                        details,
                        baggingHistory.getHistory(),
                        bridgeContext);
            }
        } else {
            contextLogger.info("Snapshot {} has no history, ignoring", snapshotId);
        }
    }

}
