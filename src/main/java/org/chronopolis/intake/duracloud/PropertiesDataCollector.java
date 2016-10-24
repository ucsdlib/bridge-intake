package org.chronopolis.intake.duracloud;

import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Read bag data from the .collection.properties file in a snapshot
 *
 * Created by shake on 7/30/15.
 */
public class PropertiesDataCollector implements DataCollector {
    private final Logger log = LoggerFactory.getLogger(PropertiesDataCollector.class);

    private final String FILE = ".collection-snapshot.properties";
    private final String PROPERTY_MEMBER_ID = "member-id";
    private final String PROPERTY_OWNER_ID = "owner-id";
    private final String PROPERTY_SPACE_ID = "duracloud-space-id";

    private IntakeSettings settings;

    @Autowired
    public PropertiesDataCollector(IntakeSettings settings) {
        this.settings = settings;
    }

    @Override
    public BagData collectBagData(String snapshotId) {
        BagData data = new BagData();
        Properties properties = new Properties();
        Path propertiesPath = Paths.get(settings.getDuracloudSnapshotStage(), snapshotId, FILE);
        try {
            InputStream is = Files.newInputStream(propertiesPath);
            properties.load(is);
            is.close();

            data.setSnapshotId(snapshotId);
            data.setName(properties.getProperty(PROPERTY_SPACE_ID, "NAME_PLACEHOLDER"));
            data.setMember(properties.getProperty(PROPERTY_MEMBER_ID, "MEMBER_PLACEHOLDER"));
            data.setDepositor(properties.getProperty(PROPERTY_OWNER_ID, "DEPOSITOR_PLACEHOLDER"));

        } catch (IOException e) {
            log.info("Error reading from properties file {}", propertiesPath);
            throw new RuntimeException(e);
        }

        return data;
    }
}
