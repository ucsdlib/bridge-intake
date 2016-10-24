package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.OnDiskTagFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Collapse under the weight of time
 *
 * Created by shake on 5/12/16.
 */
public class DuracloudMD5 extends OnDiskTagFile {
    private final Logger log = LoggerFactory.getLogger(DuracloudMD5.class);

    private Predicate<String> predicate;
    private List<String> collection;
    private Long size;
    private final String path;

    public DuracloudMD5(Path tag) {
        super(tag);
        this.path = tag.toString();
    }

    // TODO: Can probably combine this + update stream and discard the predicate when we're done
    public void setPredicate(Predicate<String> predicate) {
        this.predicate = predicate;
        updateStream();
    }

    private void updateStream() {
        // Make sure our file gets closed
        try (Stream<String> s = Files.lines(Paths.get(path))) {
            collection = s.filter(predicate)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Error reading duracloud md5 manifest");

            // ...? Not sure of the best way to handle this
            throw new RuntimeException("");
        }

        size = collection.stream().reduce(0L, (l, s) -> l + (s + "\n").length(), (l, r) -> l + r);
    }

    @Override
    public long getSize() {
        if (size != null) {
            return size;
        }

        return super.getSize();
    }

    @Override
    public InputStream getInputStream() {
        if (collection != null) {
            return new IteratorInputStream(collection.iterator());
        }

        return super.getInputStream();
    }

    class IteratorInputStream extends InputStream {

        Iterator<String> iterator;
        ByteBuffer current;

        public IteratorInputStream(Iterator<String> iterator) {
            this.iterator = iterator;
        }

        @Override
        public int read() throws IOException {
            if ((current == null || !current.hasRemaining()) && !iterator.hasNext()) {
                return -1;
            } else if ((current == null || !current.hasRemaining()) && iterator.hasNext()) {
                String next = iterator.next() + "\n";
                current = ByteBuffer.wrap(next.getBytes());
            }

            return current.get();
        }

        @Override
        public int read(byte[] b) throws IOException {
           current.get(b);
           return b.length;
        }

        /*
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return 0;
        }
        */


    }

}
