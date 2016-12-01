package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.batch.CallWrapper;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.domain.PageImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * Created by shake on 12/1/16.
 */
public class CleanerTestChron extends CleanerTest {
    private final String name = "test-name";
    private final String depositor = "test-depositor";

    @Test
    public void clean() throws Exception {
        settings.setCleanDryRun(false);
        Path bag = doChronClean(BagStatus.PRESERVED);
        Assert.assertTrue(Files.notExists(bag));
    }

    @Test
    public void cleanDryRun() throws Exception {
        settings.setCleanDryRun(true);
        Path bag = doChronClean(BagStatus.PRESERVED);
        Assert.assertTrue(Files.exists(bag));
    }

    @Test
    public void cleanNotReplicated() throws Exception {
        settings.setCleanDryRun(false);
        Path bag = doChronClean(BagStatus.REPLICATING);
        Assert.assertTrue(Files.exists(bag));
    }

    private Path doChronClean(BagStatus status) throws IOException {
        String uuid = UUID.randomUUID().toString();
        Path bag = Files.createDirectory(tmp.resolve(uuid));
        when(ingest.getBags(anyMap())).thenReturn(new CallWrapper<>(bag(status)));
        cleaner.chron(bag);
        verify(ingest, times(1)).getBags(anyMap());
        return bag;
    }

    private PageImpl<Bag> bag(BagStatus status) {
        Bag b = new Bag();
        b.setName(name)
         .setDepositor(depositor)
         .setStatus(status);
        return new PageImpl<>(ImmutableList.of(b));
    }

    @Test
    public void isSnapshot() throws Exception {
        Path fromOther = bags.resolve(FROM_OTHER);
        Path fromSnapshot = bags.resolve(FROM_SNAPSHOT);

        Assert.assertFalse(cleaner.isSnapshot(fromOther));
        Assert.assertTrue(cleaner.isSnapshot(fromSnapshot));
    }

}