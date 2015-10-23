package org.chronopolis.intake.duracloud.scheduled;

import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.Snapshot;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStatus;
import org.chronopolis.intake.duracloud.remote.model.Snapshots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Define a scheduled task which polls the Bridge server for snapshots
 *
 *
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

    @Scheduled(cron = "0 * * * * *")
    public void findSnapshots() {
        log.trace("Polling for snapshots...");
        Snapshots snapshots = bridge.getSnapshots(null);
        for (Snapshot snapshot : snapshots.getSnapshots()) {
            String snapshotId = snapshot.getSnapshotId();
            if (snapshot.getStatus() == SnapshotStatus.WAITING_FOR_DPN) {
                log.info("Bagging snapshot ", snapshotId);
                SnapshotDetails details = bridge.getSnapshotDetails(snapshotId);

                // bag and push to chron/dpn
                manager.startSnapshotTasklet(details);
            }

        }
    }

    // I don't think we need this - depends on when we close snapshots
    public void updateSnapshots() {
    }

}
