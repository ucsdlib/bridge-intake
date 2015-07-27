package org.chronopolis.intake.duracloud.scheduled;

import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.Snapshot;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
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
        List<Snapshot> snapshots = bridge.getSnapshots(null);
        for (Snapshot snapshot : snapshots) {
            String snapshotId = snapshot.getSnapshotId();
            log.info("Bagging snapshot ", snapshotId);

            if (snapshot.getStatus() == SnapshotStatus.WAITING_FOR_DPN) {
                SnapshotDetails details = bridge.getSnapshotDetails(snapshotId);
                // push snapshot to chron
                manager.startSnapshotTasklet(details);
            }

        }
    }

    @Scheduled(cron = "5 * * * * *")
    public void updateSnapshots() {
    }

}
