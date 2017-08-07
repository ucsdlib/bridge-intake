package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.config.props.Chron;
import org.chronopolis.intake.duracloud.config.props.Duracloud;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * Created by shake on 5/4/16.
 */
public class BaggingTaskletTest {
    private final Logger log = LoggerFactory.getLogger(BaggingTaskletTest.class);

    private BridgeAPI bridge;
    private Notifier notifier;
    private BaggingTasklet tasklet;
    private IntakeSettings settings;
    private BagProperties bagProperties;

    @Before
    public void setup() throws URISyntaxException {
        // setup
        bagProperties = new BagProperties();
        URL resources = ClassLoader.getSystemClassLoader().getResource("");
        Path bags = Paths.get(resources.toURI()).resolve("bags");
        Path snapshots = Paths.get(resources.toURI()).resolve("snapshots");

        settings = new IntakeSettings();
        settings.setPushDPN(true);
        Chron chron = new Chron();
        Duracloud dc = new Duracloud();
        chron.setBags(bags.toString());
        dc.setSnapshots(snapshots.toString());
        dc.setManifest("manifest-sha256.txt");
        settings.setChron(chron);
        settings.setDuracloud(dc);

        // http calls can be mocked
        bridge = mock(BridgeAPI.class);
        notifier = mock(Notifier.class);
    }

    @Test
    public void testBagger() throws IOException {
        String id = "test-snapshot";
        String name = "test";
        String depositor = "test-depositor";

        tasklet = new BaggingTasklet(id, depositor, settings, bagProperties, bridge, notifier);
        when(bridge.postHistory(eq("test-snapshot"), any(History.class))).thenReturn(new CallWrapper<>(new HistorySummary()));

        try {
            tasklet.execute(null, null);
        } catch (Exception e) {
            log.error("", e);
        }

        verify(bridge, times(1)).postHistory(eq("test-snapshot"), any(History.class));
    }

    @Test
    public void testEmptyBagger() throws IOException {
        String id = "empty-snapshot";
        String name = "test";
        String depositor = "test-depositor";

        tasklet = new BaggingTasklet(id, depositor, settings, bagProperties, bridge, notifier);

        try {
            tasklet.execute(null, null);
        } catch (Exception e) {
            log.error("", e);
        }

        verify(bridge, times(0)).postHistory(eq("test-snapshot"), any(History.class));
    }
}
