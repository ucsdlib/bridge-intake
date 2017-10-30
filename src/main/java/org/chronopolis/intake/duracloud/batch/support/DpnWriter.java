package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.packager.Packager;
import org.chronopolis.bag.writer.BagWriter;
import org.chronopolis.bag.writer.SimpleBagWriter;
import org.chronopolis.bag.writer.WriteJob;
import org.chronopolis.bag.writer.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Extension of the MultipartWriter that adds a DpnInfo file into Bags
 *
 * Created by shake on 2/19/16.
 */
public class DpnWriter extends SimpleBagWriter {
    private final Logger log = LoggerFactory.getLogger(DpnWriter.class);

    private final String depositor;
    private final String snapshotId;
    private boolean validate;
    private Packager packager;

    public DpnWriter(String depositor, String snapshotId) {
        super();

        this.depositor = depositor;
        this.snapshotId = snapshotId;
    }

    @Override
    public BagWriter validate(boolean validate) {
        this.validate = validate;
        return this;
    }

    @Override
    public BagWriter withPackager(Packager packager) {
        this.packager = packager;
        return this;
    }

    @Override
    public List<WriteResult> write(List<Bag> bags) {
        return bags.stream()
            .peek(this::updateMd5s)
            .peek(b -> b.addTag(createDpnInfo(b)))
            .map(this::fromBag)
            .collect(Collectors.toList());
    }

    private WriteResult fromBag(Bag bag) {
        WriteJob job = new WriteJob(bag, true, validate, packager);
        return job.get();
    }

    private void updateMd5s(Bag b) {
        // This is kind of obnoxious, it would be nice to extend bag
        // so that we can simply get the value after
        log.debug("Updating md5 manifest for {}", b.getName());
        Path md5 = Paths.get("manifest-md5.txt");
        TagFile dura = b.getTags().get(md5);
        if (dura != null && dura instanceof DuracloudMD5) {
            ifPresent(b, (DuracloudMD5) dura);
        }
    }

    private void ifPresent(Bag b, DuracloudMD5 md5) {
        md5.setPredicate(s -> {
            String[] split = s.split("\\s+", 2);
            if (split.length != 2) {
                return false;
            }

            String path = split[1];
            return b.getFiles().containsKey(Paths.get(path));
       });
    }

    private TagFile createDpnInfo(Bag b) {
        log.debug("Adding dpn-info for {}", b.getName());
        DpnInfo dpnInfo = new DpnInfo();

        // ex: chron://ucsd/some-ucsd-dpn-snapshot/0
        // todo: BagProperties to set DpnInfo/BagInfo
        String local = "chron://" + depositor + "/" + snapshotId + "/" + b.getNumber();
        dpnInfo.withInfo(DpnInfo.Tag.INGEST_NODE_CONTACT_NAME, "Sibyl Schaefer")
               .withInfo(DpnInfo.Tag.INGEST_NODE_CONTACT_EMAIL, "sschaefer@uscd.edu")
               .withInfo(DpnInfo.Tag.INGEST_NODE_CONTACT_EMAIL, "chronopolis-support-l@mailman.ucsd.edu")
               .withInfo(DpnInfo.Tag.INGEST_NODE_ADDRESS, "University of California, San Diego, 9500 Gilman Dr, La Jolla, CA 92093")
               .withInfo(DpnInfo.Tag.INGEST_NODE_NAME, "Chronopolis")
               .withInfo(DpnInfo.Tag.DPN_OBJECT_ID, b.getName())
               .withInfo(DpnInfo.Tag.FIRST_VERSION_OBJECT_ID, b.getName())
               .withInfo(DpnInfo.Tag.VERSION_NUMBER, String.valueOf(1))
               .withInfo(DpnInfo.Tag.LOCAL_ID, local)
               .withInfo(DpnInfo.Tag.BAG_TYPE, "data")
               // Empty entries for the inter/rights for now
               .withInfo(DpnInfo.Tag.RIGHTS_OBJECT_ID, "")
               .withInfo(DpnInfo.Tag.INTERPRETIVE_OBJECT_ID, "");
        return dpnInfo;
    }
}
