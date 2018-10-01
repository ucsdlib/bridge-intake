package org.chronopolis.intake.duracloud.batch.bagging;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.test.support.CallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * mawp
 *
 * Created by shake on 5/4/16.
 */
public class BaggingTaskletTest {
    private final Logger log = LoggerFactory.getLogger(BaggingTaskletTest.class);

    private BridgeAPI bridge;
    private Notifier notifier;
    private BridgeContext context;
    private BaggingTasklet tasklet;
    private BagProperties bagProperties;
    private BagStagingProperties stagingProperties;

    @Before
    public void setup() throws URISyntaxException {
        // setup
        bagProperties = new BagProperties();
        String prefix = "";
        String manifest = "manifest-sha256.txt";
        URL resources = ClassLoader.getSystemClassLoader().getResource("");
        Assert.assertNotNull(resources);

        String bags = Paths.get(resources.toURI()).resolve("bags").toString();
        String snapshots = Paths.get(resources.toURI()).resolve("snapshots").toString();

        stagingProperties = new BagStagingProperties();
        stagingProperties.setPosix(new Posix().setPath(bags));

        // http calls can be mocked
        bridge = mock(BridgeAPI.class);
        notifier = mock(Notifier.class);

        context= new BridgeContext(bridge, prefix, manifest, snapshots, snapshots, Push.DPN);
    }

    @Test
    public void testBagger() {
        String id = "test-snapshot";
        String depositor = "test-depositor";

        tasklet = new BaggingTasklet(id,
                depositor, context, bagProperties, stagingProperties, notifier);
        when(bridge.postHistory(eq("test-snapshot"), any(History.class)))
                .thenReturn(new CallWrapper<>(new HistorySummary()));

        try {
            tasklet.run();
        } catch (Exception e) {
            log.error("", e);
        }

        verify(bridge, times(1)).postHistory(eq("test-snapshot"), any(History.class));
    }

    @Test
    public void testEmptyBagger() {
        String id = "empty-snapshot";
        String depositor = "test-depositor";

        tasklet = new BaggingTasklet(id,
                depositor, context, bagProperties, stagingProperties, notifier);

        try {
            tasklet.run();
        } catch (Exception e) {
            log.error("", e);
        }

        verify(bridge, times(0)).postHistory(eq("test-snapshot"), any(History.class));
    }
}
