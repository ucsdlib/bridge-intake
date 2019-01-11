package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.OnDiskTagFile;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

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
 * TODO: This isn't serializable which can mess with the BagWriter
 *       we'll want to get that in check
 *
 * Created by shake on 5/12/16.
 */
public class DuracloudMD5 extends OnDiskTagFile {
    private final Logger log;

    private final String path;
    private transient Predicate<String> predicate;

    private Long size;
    private List<String> collection;

    public DuracloudMD5(Path tag, BridgeContext context) {
        super(tag);
        this.path = tag.toString();
        this.log = context.getLogger();
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

        size = collection.stream()
                .reduce(0L, (l, s) -> l + (s + "\n").getBytes().length, (l, r) -> l + r);
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

        private Iterator<String> iterator;
        private ByteBuffer current;

        public IteratorInputStream(Iterator<String> iterator) {
            this.iterator = iterator;
        }

        @Override
        public int read() {
            if ((current == null || !current.hasRemaining()) && !iterator.hasNext()) {
                return -1;
            } else if ((current == null || !current.hasRemaining()) && iterator.hasNext()) {
                String next = iterator.next() + "\n";
                current = ByteBuffer.wrap(next.getBytes());
            }

            return current.get();
        }

        @Override
        public int read(@NotNull byte[] b) {
           current.get(b);
           return b.length;
        }
    }

}
