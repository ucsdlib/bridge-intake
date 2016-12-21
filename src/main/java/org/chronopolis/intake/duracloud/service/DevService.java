package org.chronopolis.intake.duracloud.service;

import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.intake.duracloud.scheduled.Cleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * Created by shake on 3/1/16.
 */
@Component
@Profile("develop")
public class DevService implements ChronService {

    private final Logger log = LoggerFactory.getLogger(DevService.class);

    final Bridge bridge;
    final Cleaner cleaner;
    final SnapshotJobManager manager;
    final IntakeSettings settings;

    @Autowired
    public DevService(Bridge bridge, Cleaner cleaner, SnapshotJobManager manager, IntakeSettings settings) {
        this.bridge = bridge;
        this.cleaner = cleaner;
        this.manager = manager;
        this.settings = settings;
    }

    @Override
    public void run() {

        boolean done = false;
        System.out.println("Enter 'q' to quit; 'p' or 'b' to poll the bridge server");
        while (!done) {
            String input = readLine();
            if ("q".equalsIgnoreCase(input)) {
                done = true;
            } else if ("tb".equalsIgnoreCase(input)) {
                testBag();
            } else if ("td".equalsIgnoreCase(input)) {
                testDpn();
            } else if ("tc".equalsIgnoreCase(input))  {
                testClean();
            } else if ("p".equalsIgnoreCase(input) || "b".equalsIgnoreCase(input)) {
                try {
                    bridge.findSnapshots();
                } catch (Exception e) {
                    log.error("Error calling bridge!", e);
                }
            }
        }
    }

    private void testClean() {
        cleaner.cleanDpn();
        cleaner.cleanChron();
    }

    // Test based on some static content
    private void testBag() {
        System.out.println("Enter snapshot id for snapshot to bag");
        /*
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(snapshotId);
        try {
            manager.startSnapshotTasklet(details);
        } catch (Exception e) {
            log.warn("Error testing", e);
        }
        */
    }

    private void testDpn() {
        log.info("Enter snapshot id to push to dpn");
        /*
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(snapshotId);

        List<BagReceipt> receipts = ImmutableList.of(
                new BagReceipt()
                        .setName("216f5fe0-0bd7-4754-b974-b9e3182e7272")
                        .setReceipt("b111b6950fea6b0728365270f79cd64ecfa8be8b5620b5e9fc9e17a620eb57bd"));

        try {
            manager.startReplicationTasklet(details, receipts, settings);
        } catch (Exception e) {
            log.warn("Error testing", e);
        }
        */
    }



    private String readLine() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            return reader.readLine();
        } catch (IOException ex) {
            throw new RuntimeException("Can't read from STDIN");
        }
    }


}
