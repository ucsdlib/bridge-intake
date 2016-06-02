package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.Digest;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.chronopolis.bag.writer.TarPackager;
import org.chronopolis.bag.writer.UUIDNamingSchema;
import org.chronopolis.bag.writer.Writer;
import org.chronopolis.intake.duracloud.batch.support.DpnWriter;
import org.chronopolis.intake.duracloud.batch.support.DuracloudMD5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 *
 * Created by shake on 5/4/16.
 */
public class BaggingTaskletTest {
    private final Logger log = LoggerFactory.getLogger(BaggingTaskletTest.class);

    // @Test
    public void testBagger() throws IOException {
        Path base = Paths.get("/export/gluster/test-bags");
        Path out = base.resolve("out");
        // Path dirOut = out.resolve("myDPNBag");
        // Path tag = base.resolve("in/tag-1.txt");
        Path payload = base.resolve("in/manifest-sha256.txt");
        PayloadManifest payloadManifest = PayloadManifest.loadFromStream(Files.newInputStream(payload), base.resolve("in"));
        BagInfo info = new BagInfo()
                .includeMissingTags(true)
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "shake@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "ekash@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_NAME, "shake")
                .withInfo(BagInfo.Tag.INFO_CONTACT_PHONE, "phone")
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, "umiacs");

        Writer writer = new DpnWriter()
                .withMaxSize(100, Unit.MEGABYTE)
                .withPayloadManifest(payloadManifest)
                .withBagIt(new BagIt())
                .withBagInfo(info)
                // .withNamingSchema(new SimpleNamingSchema("mpDPNBag"))
                .withNamingSchema(new UUIDNamingSchema())
                // .withPackager(new InPlaceDirectoryPackager(out.resolve("mpDPNBag")))
                // .withPackager(new DirectoryPackager(out))
                .withPackager(new TarPackager(out))
                .withDigest(Digest.SHA_256)
                // .withTagFile(new OnDiskTagFile(tag))
                .withTagFile(new DuracloudMD5(base.resolve("in/manifest-md5.txt")));


        List<Bag> write = writer.write();

        for (Bag bag : write) {
            log.info("Wrote bag {} with receipt {}", bag.getName(), bag.getReceipt());
            if (!bag.isValid()) {
                log.info("Bag is invalid, errors are {}", bag.getErrors());
            }
        }

    }

}
