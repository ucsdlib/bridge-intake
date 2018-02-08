package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class CleanerTest {

    private Path tmp;
    private BagStagingProperties stagingProperties;

    private final String TEST_DEPOSITOR = "test";
    private final String TEST_FILE = "hello-world";
    private final String FROM_DIR = "94400957-cb72-4c19-bf07-6476c5a3a60d";
    private final String FROM_TAR = "5309da6f-c1cc-40ad-be42-e67e722cce04";

    @Before
    public void setup() throws IOException, URISyntaxException {
        tmp = Files.createTempDirectory("cleanertest");
        stagingProperties = new BagStagingProperties()
                .setPosix(new Posix().setPath(tmp.toString()));
    }

    @Test
    public void cleanDirectoryRemoveParent() throws IOException {
        // initial setup
        Path depositorLevel = tmp.resolve(TEST_DEPOSITOR);
        Path bagLevel = depositorLevel.resolve(FROM_DIR);
        Files.createDirectories(bagLevel);
        Files.createFile(bagLevel.resolve(TEST_FILE));
        // one more level
        Files.createDirectories(bagLevel.resolve(FROM_DIR));
        Files.createFile(bagLevel.resolve(FROM_DIR).resolve(TEST_FILE));

        Cleaner cleaner = new Cleaner(Paths.get(TEST_DEPOSITOR, FROM_DIR), stagingProperties);
        cleaner.call();

        Assert.assertTrue(Files.notExists(bagLevel.resolve(TEST_FILE)));
        Assert.assertTrue(Files.notExists(bagLevel));
        Assert.assertTrue(Files.notExists(depositorLevel));
        Assert.assertTrue(Files.exists(tmp));
    }

    @Test
    public void cleanDirectoryKeepParent() throws IOException {
        Path depositorLevel = tmp.resolve(TEST_DEPOSITOR);
        Path bagLevel = depositorLevel.resolve(FROM_DIR);
        Files.createDirectories(bagLevel);
        Files.createFile(bagLevel.resolve(TEST_FILE));
        Path extra = depositorLevel.resolve(UUID.randomUUID().toString());
        Files.createDirectories(depositorLevel.resolve(extra));

        Cleaner cleaner = new Cleaner(Paths.get(TEST_DEPOSITOR, FROM_DIR), stagingProperties);
        cleaner.call();

        Assert.assertTrue(Files.notExists(bagLevel.resolve(TEST_FILE)));
        Assert.assertTrue(Files.notExists(bagLevel));
        Assert.assertTrue(Files.exists(depositorLevel));
        Assert.assertTrue(Files.exists(extra));
        Assert.assertTrue(Files.exists(tmp));
    }

    @Test
    public void cleanDirectoryNotExists() {
        Cleaner cleaner = new Cleaner(Paths.get(TEST_DEPOSITOR, UUID.randomUUID().toString()), stagingProperties);
        cleaner.call();
    }

    @Test
    public void cleanTarball() throws IOException {
        Path depositorLevel = tmp.resolve(TEST_DEPOSITOR);
        Path bag = depositorLevel.resolve(FROM_TAR + ".tar");
        Files.createDirectories(depositorLevel);
        Files.createFile(bag);

        Cleaner cleaner = new Cleaner(Paths.get(TEST_DEPOSITOR, FROM_TAR + ".tar"), stagingProperties);
        cleaner.call();

        Assert.assertTrue(Files.notExists(bag));
        Assert.assertTrue(Files.notExists(depositorLevel));
        Assert.assertTrue(Files.exists(tmp));
    }

    // what exactly should this be testing?
    public void cleanNonExistent() {

    }

}