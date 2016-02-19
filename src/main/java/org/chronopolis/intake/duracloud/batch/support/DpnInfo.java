package org.chronopolis.intake.duracloud.batch.support;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.chronopolis.bag.core.TagFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 *
 * Created by shake on 2/18/16.
 */
public class DpnInfo implements TagFile {
    private final Logger log = LoggerFactory.getLogger(DpnInfo.class);

    enum Tag {
        DPN_OBJECT_ID("DPN-Object-ID"),
        LOCAL_ID("Local-ID"),
        INGEST_NODE_NAME("Ingest-Node-Name"),
        INGEST_NODE_ADDRESS("Ingest-Node-Address"),
        INGEST_NODE_CONTACT_NAME ("Ingest-Node-Contact-Name"),
        INGEST_NODE_CONTACT_EMAIL("Ingest-Node-Contact-Email"),
        VERSION_NUMBER("Version-Number"),
        FIRST_VERSION_OBJECT_ID("First-Version-Object-ID"),
        INTERPRETIVE_OBJECT_ID("Interpretive-Object-ID"),
        RIGHTS_OBJECT_ID("Rights-Object-ID"),
        BAG_TYPE("Bag-Type")
        ;

        private final String name;

        Tag(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private Multimap<Tag, String> tags = ArrayListMultimap.create();
    private final Path path;

    public DpnInfo() {
        this.path = Paths.get("dpn-tags/dpn-info.txt");
    }

    public DpnInfo withInfo(Tag tag, String value) {
        tags.put(tag, value);
        return this;
    }

    @Override
    public long getSize() {
        long size = 0;
        for (Map.Entry<Tag, String> entry : tags.entries()) {
            String tag = entry.getKey().getName() +
                    ": " +
                    entry.getValue() +
                    "\r\n";
            size += tag.length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public InputStream getInputStream() {
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream os = new PipedOutputStream();

        try {
            is.connect(os);
            for (Map.Entry<Tag, String> entry : tags.entries()) {
                String tag = entry.getKey().getName() +
                        ": " +
                        entry.getValue() +
                        "\r\n";
                os.write(tag.getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error writing DpnInfo InputStream", e);
        }
        return is;
    }

}
