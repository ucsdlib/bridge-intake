package org.chronopolis.intake.duracloud;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Read a dpn info file from any type of source and I need food
 *
 * Created by shake on 11/12/15.
 */
public class DpnInfoReader {
    private static final Logger log = LoggerFactory.getLogger(DpnInfoReader.class);
    public static final String DPN_INFO = "dpn-tags/dpn-info.txt";

    final ImmutableMultimap<Tag, String> tags;

    private DpnInfoReader(ImmutableMultimap<Tag, String> tags) {
        this.tags = tags;
    }

    public static DpnInfoReader read(Path bag) throws IOException {
        Path info = bag.resolve(DPN_INFO);
        BufferedReader reader = Files.newBufferedReader(info, Charset.defaultCharset());
        return read(reader);
    }

    public static DpnInfoReader read(TarArchiveInputStream is, String root) throws IOException {
        String dpnInfo = root + "/" + DPN_INFO;

        TarArchiveEntry entry;
        while ((entry = is.getNextTarEntry()) != null) {
            if (entry.getName().equals(dpnInfo)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                    return read(reader);
                }
            }
        }

        // Empty dpninfo?
        return new DpnInfoReader(ImmutableMultimap.of());
    }

    // TODO: autoclose/close br
    private static DpnInfoReader read(BufferedReader r) throws IOException {
        ImmutableMultimap.Builder<Tag, String> builder = ImmutableMultimap.builder();
        String line;
        Tag t = Tag.UNKNOWN; // init as unknown just in case
        while ((line = r.readLine()) != null) {
            String[] split = line.split(":\\s", 2);
            String val;
            if (split.length > 1) {
                t = Tag.valueOf(split[0].replace("-", "_").toUpperCase());
                val = split[1];
            } else {
                val = split[0];
            }

            // ignore empty tags
            if (!val.isEmpty()) {
                builder.put(t, val);
            }
        }

        return new DpnInfoReader(builder.build());
    }

    public ImmutableMultimap<Tag, String> getTags() {
        return tags;
    }

    public String getFirstVersionUUID() {
        ImmutableCollection<String> strings = tags.get(Tag.FIRST_VERSION_OBJECT_ID);
        return getConcatenatedEntry(strings);
    }

    public String getLocalId() {
        ImmutableCollection<String> strings = tags.get(Tag.LOCAL_ID);
        return getConcatenatedEntry(strings);
    }



    public String getUUID() {
        return getConcatenatedEntry(tags.get(Tag.DPN_OBJECT_ID));
    }

    public String getIngestNodeName() {
        return getConcatenatedEntry(tags.get(Tag.INGEST_NODE_NAME));
    }

    public String getIngestNodeAddress() {
        return getConcatenatedEntry(tags.get(Tag.INGEST_NODE_ADDRESS));
    }

    public String getIngestNodeContactName() {
        return getConcatenatedEntry(tags.get(Tag.INGEST_NODE_CONTACT_NAME));
    }

    public String getIngestNodeContactEmail() {
        return getConcatenatedEntry(tags.get(Tag.INGEST_NODE_CONTACT_EMAIL));
    }

    public String getBagType() {
        return getConcatenatedEntry(tags.get(Tag.BAG_TYPE));
    }

    public Long getVersionNumber() {
        ImmutableCollection<String> strings = tags.get(Tag.VERSION_NUMBER);
        return Long.valueOf(strings.asList().get(0));
    }

    public List<String> getRightsIds() {
        return tags.get(Tag.RIGHTS_OBJECT_ID).asList();
    }

    public List<String> getInterpretiveIds() {
        return tags.get(Tag.INTERPRETIVE_OBJECT_ID).asList();
    }

    private String getConcatenatedEntry(ImmutableCollection<String> strings) {
        if (strings.isEmpty()) {
            return null;
        }

        StringBuilder concat = new StringBuilder();
        for (String s : strings) {
            concat.append(s);
        }
        return concat.toString();
    }

    public enum Tag {
        DPN_OBJECT_ID,
        BAGGING_DATE,
        LOCAL_ID,
        INGEST_NODE_NAME,
        INGEST_NODE_ADDRESS,
        INGEST_NODE_CONTACT_NAME,
        INGEST_NODE_CONTACT_EMAIL,
        VERSION_NUMBER,
        FIRST_VERSION_OBJECT_ID,
        BAG_TYPE,
        RIGHTS_OBJECT_ID,
        INTERPRETIVE_OBJECT_ID,
        UNKNOWN
    }

}
