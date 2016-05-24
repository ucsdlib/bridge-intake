package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.writer.MultipartWriter;
import org.chronopolis.bag.writer.Writer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

/**
 * Extension of the MultipartWriter that adds a DpnInfo file into Bags
 *
 * Created by shake on 2/19/16.
 */
public class DpnWriter extends MultipartWriter {

    private String depositor;

    public DpnWriter() {
        super();
    }

    @Override
    public List<Bag> write() {
        // preprocess and bags from super
        preprocess();
        int idx = 0;
        int total = bags.size();
        for (Bag bag : bags) {
            bag.setGroupTotal(total);
            bag.prepareForWrite();

            // We need the name to be set prior to creating the dpn-info
            String name = namingSchema.getName(idx);
            bag.setName(name);

            // Create the dpn info + update the bags md5 manifest
            addDpnInfo(bag);
            updateMd5s(bag);

            writeBag(bag);
            idx++;
        }

        return bags;
    }

    private void updateMd5s(Bag b) {
        Path md5 = Paths.get("manifest-md5.txt");
        b.getTags().values().stream()
                .filter(t -> t.getPath().equals(md5))
                .map(t -> {
                    // Create a List<Optional<DuracloudMD5>>
                    Optional<DuracloudMD5> optional;
                    if (t instanceof DuracloudMD5) {
                        optional = Optional.of((DuracloudMD5) t);
                    } else {
                        optional = Optional.empty();
                    }
                    return optional;
                })
                .forEach(o -> o.ifPresent(m -> ifPresent(b, m)));
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

    private void addDpnInfo(Bag b) {
        DpnInfo dpnInfo = new DpnInfo();

        String local = "chron://" + depositor + "/" + b.getName();
        dpnInfo.withInfo(DpnInfo.Tag.INGEST_NODE_CONTACT_NAME, "Sibyl Schaefer")
               .withInfo(DpnInfo.Tag.INGEST_NODE_CONTACT_EMAIL, "sschaefer@uscd.edu")
               .withInfo(DpnInfo.Tag.INGEST_NODE_ADDRESS, "ucsd")
               .withInfo(DpnInfo.Tag.INGEST_NODE_NAME, "chron")
               .withInfo(DpnInfo.Tag.DPN_OBJECT_ID, b.getName())
               .withInfo(DpnInfo.Tag.FIRST_VERSION_OBJECT_ID, b.getName())
               .withInfo(DpnInfo.Tag.VERSION_NUMBER, String.valueOf(1))
               .withInfo(DpnInfo.Tag.LOCAL_ID, local)
               .withInfo(DpnInfo.Tag.BAG_TYPE, "data")
               // Empty entries for the inter/rights for now
               .withInfo(DpnInfo.Tag.RIGHTS_OBJECT_ID, "")
               .withInfo(DpnInfo.Tag.INTERPRETIVE_OBJECT_ID, "");

        b.addTag(dpnInfo);
    }

    public Writer withDepositor(String depositor) {
        this.depositor = depositor;
        return this;
    }
}
