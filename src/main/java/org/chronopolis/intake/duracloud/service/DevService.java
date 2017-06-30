package org.chronopolis.intake.duracloud.service;

import org.chronopolis.bag.UUIDNamingSchema;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.OnDiskTagFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.chronopolis.bag.packager.TarPackager;
import org.chronopolis.bag.partitioner.Bagger;
import org.chronopolis.bag.partitioner.BaggingResult;
import org.chronopolis.bag.writer.BagWriter;
import org.chronopolis.bag.writer.WriteResult;
import org.chronopolis.intake.duracloud.batch.support.DpnWriter;
import org.chronopolis.intake.duracloud.batch.support.DuracloudMD5;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.intake.duracloud.scheduled.Cleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.chronopolis.intake.duracloud.batch.BaggingTasklet.SNAPSHOT_COLLECTION_PROPERTIES;
import static org.chronopolis.intake.duracloud.batch.BaggingTasklet.SNAPSHOT_CONTENT_PROPERTIES;
import static org.chronopolis.intake.duracloud.batch.BaggingTasklet.SNAPSHOT_MD5;

/**
 *
 * Created by shake on 3/1/16.
 */
@Component
@Profile("develop")
public class DevService implements ChronService {

    private final Logger log = LoggerFactory.getLogger(DevService.class);

    private final Bridge bridge;
    private final Cleaner cleaner;
    // private final SnapshotJobManager manager;
    // private final IntakeSettings settings;

    @Autowired
    public DevService(Bridge bridge, Cleaner cleaner) {
        this.bridge = bridge;
        this.cleaner = cleaner;
        // this.manager = manager;
        // this.settings = settings;
    }

    @Override
    public void run() {

        boolean done = false;
        System.out.println("Enter 'q' to quit; 'p' or 'b' to poll the bridge server");
        while (!done) {
            String input = readLine();
            if ("q".equalsIgnoreCase(input)) {
                done = true;
            } else if ("tb".equalsIgnoreCase(input)) {
                testBag();
            } else if ("td".equalsIgnoreCase(input)) {
                testDpn();
            } else if ("tc".equalsIgnoreCase(input))  {
                testClean();
            } else if ("p".equalsIgnoreCase(input) || "b".equalsIgnoreCase(input)) {
                try {
                    bridge.findSnapshots();
                } catch (Exception e) {
                    log.error("Error calling bridge!", e);
                }
            }
        }
    }

    private void testClean() {
        cleaner.cleanDpn();
        cleaner.cleanChron();
    }

    // Test based on some static content
    private void testBag() {
        System.out.println("Enter snapshot id for snapshot to bag");
        Path snapshotBase = Paths.get("/export/gluster/test-bags/tufts_1106_ms208-mobius_2017-01-10-15-55-24");
        String manifestName = "manifest-sha256.txt";

        PayloadManifest manifest = null;
        try {
            manifest = PayloadManifest.loadFromStream(
                    Files.newInputStream(snapshotBase.resolve(manifestName)),
                    snapshotBase);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TODO: fill out with what...?
        // TODO: EXTERNAL-IDENTIFIER: snapshot.description
        BagInfo info = new BagInfo()
                .includeMissingTags(true)
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, "tufts");

        Bagger bagger = new Bagger()
                .withBagInfo(info)
                .withBagit(new BagIt())
                .withPayloadManifest(manifest)
                .withTagFile(new DuracloudMD5(snapshotBase.resolve(SNAPSHOT_MD5)))
                .withTagFile(new OnDiskTagFile(snapshotBase.resolve(SNAPSHOT_CONTENT_PROPERTIES)))
                .withTagFile(new OnDiskTagFile(snapshotBase.resolve(SNAPSHOT_COLLECTION_PROPERTIES)));
        bagger = bagger
                .withMaxSize(240, Unit.GIGABYTE)
                .withNamingSchema(new UUIDNamingSchema());

        BaggingResult partition = bagger.partition();

        BagWriter writer = new DpnWriter("tufts", "tufts_1106_ms208-mobius_2017-01-10-15-55-24")
                .withPackager(new TarPackager(Paths.get("/export/gluster/test-bags/tufts-test")));
        partition.getBags().forEach(b ->
                log.info("{} -> ({} Files, {} Size)",
                        new Object[]{b.getName(), b.getManifest().getFiles().size(), b.getManifest().getSize()}));
        List<WriteResult> results = writer.write(partition.getBags());
        results.forEach(this::printInfo);

        log.info("{}", Charset.defaultCharset());

        /*
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(snapshotId);
        try {
            manager.startSnapshotTasklet(details);
        } catch (Exception e) {
            log.warn("Error testing", e);
        }
        */
    }

    private void printInfo(WriteResult writeResult) {
        Bag bag = writeResult.getBag();
        bag.getManifest().getFiles().values().forEach(f -> {
            String line = f.getDigest().toString() + " " + f.getFile().toString() + "\n";
            ByteBuffer encode = Charset.forName("UTF-8").encode(line);
            ByteBuffer encodedf = Charset.defaultCharset().encode(line);
            log.info("{} -> L={} B={} E-UTF8={} E-DC={}",
                    f.getFile().toString(),
                    line.length(),
                    line.getBytes(Charset.forName("UTF-8")).length,
                    encode.limit(),
                    encodedf.limit());
        });
    }

    private void testDpn() {
        log.info("Enter snapshot id to push to dpn");
        /*
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(snapshotId);

        List<BagReceipt> receipts = ImmutableList.of(
                new BagReceipt()
                        .setName("216f5fe0-0bd7-4754-b974-b9e3182e7272")
                        .setReceipt("b111b6950fea6b0728365270f79cd64ecfa8be8b5620b5e9fc9e17a620eb57bd"));

        try {
            manager.startReplicationTasklet(details, receipts, settings);
        } catch (Exception e) {
            log.warn("Error testing", e);
        }
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
