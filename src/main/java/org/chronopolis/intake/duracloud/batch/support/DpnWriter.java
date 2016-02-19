package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.writer.MultipartWriter;
import org.chronopolis.bag.writer.Writer;

import java.util.List;

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

            addDpnInfo(bag);
            writeBag(bag);
            idx++;
        }

        return bags;
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
