package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.intake.duracloud.batch.CallWrapper;
import org.chronopolis.intake.duracloud.test.TestApplication;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.SpringApplicationConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test our processes for cleaning bags
 *
 * Created by shake on 12/1/16.
 */
@SpringApplicationConfiguration(classes = TestApplication.class)
public class CleanerTestDpn extends CleanerTest {

    private final Logger log = LoggerFactory.getLogger(CleanerTestDpn.class);

    @Test
    public void dpn() throws Exception {
        settings.setCleanDryRun(false);
        Path tar = doDpnRun(true);
        Assert.assertTrue(Files.notExists(tar));
    }

    @Test
    public void dpnNotReplicated() throws Exception {
        settings.setCleanDryRun(false);
        Path tar = doDpnRun(false);
        Assert.assertTrue(Files.exists(tar));
    }

    @Test
    public void dpnDry() throws Exception {
        settings.setCleanDryRun(true);
        Path tar = doDpnRun(true);
        Assert.assertTrue(Files.exists(tar));
    }

    private Path doDpnRun(boolean replicated) throws IOException {
        log.info("Creating bogus tar file");
        String uuid = UUID.randomUUID().toString();
        Path tar = Files.createFile(tmp.resolve(uuid + ".tar"));

        when(bag.getBag(uuid)).thenReturn(new CallWrapper<>(bag(uuid, replicated)));
        cleaner.dpn(tar);

        verify(bag, times(1)).getBag(uuid);
        return tar;
    }

    private Bag bag(String uuid, boolean replicated) {
        Bag b = new Bag();
        b.setUuid(uuid);
        if (replicated) {
            b.setReplicatingNodes(ImmutableList.of("mock-1", "mock-2", "mock-3"));
        } else {
            b.setReplicatingNodes(ImmutableList.of());
        }
        return b;
    }

    @Test
    public void isSerializedSnapshot() throws Exception {
        Path fromOther = bags.resolve(FROM_OTHER + ".tar");
        Path fromSnapshot = bags.resolve(FROM_SNAPSHOT + ".tar");

        Assert.assertFalse(cleaner.isSerializedSnapshot(fromOther));
        Assert.assertTrue(cleaner.isSerializedSnapshot(fromSnapshot));
    }

}