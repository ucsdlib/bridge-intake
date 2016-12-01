package org.chronopolis.intake.duracloud.scheduled;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.IngestAPI;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Base class for our other two tests
 *
 * Created by shake on 12/1/16.
 */
public class CleanerTest {
    @Mock BridgeAPI bridge;
    @Mock IngestAPI ingest;
    @Mock BalustradeBag bag;

    Path tmp;
    Path bags;
    Cleaner cleaner;
    IntakeSettings settings;

    final String FROM_OTHER = "94400957-cb72-4c19-bf07-6476c5a3a60d";
    final String FROM_SNAPSHOT = "5309da6f-c1cc-40ad-be42-e67e722cce04";

    @Before
    public void setup() throws IOException, URISyntaxException {
        MockitoAnnotations.initMocks(this);
        LocalAPI localAPI = new LocalAPI();
        localAPI.setBagAPI(bag);

        tmp = Files.createTempDirectory("cleanertest");
        settings = new IntakeSettings();
        cleaner = new Cleaner(bridge, ingest, localAPI, settings);

        URL resources = ClassLoader.getSystemClassLoader().getResource("");
        bags = Paths.get(resources.toURI()).resolve("bags");
    }

    @After
    public void teardown() {
        cleaner.cleanDirectory(tmp);
    }

}