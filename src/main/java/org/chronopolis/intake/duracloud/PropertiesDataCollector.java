package org.chronopolis.intake.duracloud;

import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.model.BagData;

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

    private BridgeContext bridgeContext;

    public PropertiesDataCollector(BridgeContext bridgeContext) {
        this.bridgeContext = bridgeContext;
    }

    @Override
    public BagData collectBagData(String snapshotId) throws IOException {
        String FILE = ".collection-snapshot.properties";
        String PROPERTY_SPACE_ID = "duracloud-space-id";
        String PROPERTY_OWNER_ID = "owner-id";
        String PROPERTY_MEMBER_ID = "member-id";

        BagData data = new BagData(bridgeContext.getChronopolisPrefix());
        Properties properties = new Properties();
        Path propertiesPath = Paths.get(bridgeContext.getSnapshots(), snapshotId, FILE);
        try (InputStream is = Files.newInputStream(propertiesPath)) {
            properties.load(is);
        }

        data.setSnapshotId(snapshotId);
        data.setName(properties.getProperty(PROPERTY_SPACE_ID, "NAME_PLACEHOLDER"));
        data.setMember(properties.getProperty(PROPERTY_MEMBER_ID, "MEMBER_PLACEHOLDER"));
        data.setDepositor(properties.getProperty(PROPERTY_OWNER_ID, "DEPOSITOR_PLACEHOLDER"));

        return data;
    }
}
