package org.chronopolis.intake.duracloud.service;

import org.chronopolis.intake.duracloud.scheduled.Bridge;
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

    @Autowired
    Bridge bridge;

    @Override
    public void run() {

        boolean done = false;
        System.out.println("Enter 'q' to quit; 'p' or 'b' to poll the bridge server");
        while (!done) {
            String input = readLine();
            if ("q".equalsIgnoreCase(input)) {
                done = true;
            } else if ("t".equalsIgnoreCase(input)) {
                test();
            } else if ("p".equalsIgnoreCase(input) || "b".equalsIgnoreCase(input)) {
                try {
                    bridge.findSnapshots();
                } catch (Exception e) {
                    log.error("Error calling bridge!", e);
                }
            }
        }
    }

    // Test based on some static content
    private void test() {
        /*
        log.info("Push chron: {} Push DPN: {}", settings.pushChronopolis(), settings.pushDPN());
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId("erik-3-erik-test-space-2014-02-21-20-17-58");
        manager.startSnapshotTasklet(details);
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
