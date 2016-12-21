package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.BagStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageImpl;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

/**
 * Class to handle scheduled checks of the staging area and determining
 * what to clean up
 * <p>
 * Created by shake on 10/27/16.
 */
@Component
@EnableScheduling
public class Cleaner {
    private final String TAR_TYPE = "application/x-tar";
    private final String SNAPSHOT_FILE = ".collection-snapshot.properties";
    private final Logger log = LoggerFactory.getLogger(Cleaner.class);

    final BridgeAPI bridge;
    final IngestAPI ingest;
    final BalustradeBag registry;
    final IntakeSettings settings;

    @Autowired
    public Cleaner(BridgeAPI bridge, IngestAPI ingest, LocalAPI dpn, IntakeSettings settings) {
        this.bridge = bridge;
        this.ingest = ingest;
        this.registry = dpn.getBagAPI();
        this.settings = settings;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void cleanDpn() {
        String bagStage = settings.getChron().getBags();
        Path bags = Paths.get(bagStage);
        try (Stream<Path> files = Files.find(bags, 2, this::matchTar)) {
            files.filter(this::isSerializedSnapshot)
                    .forEach(this::dpn);
        } catch (IOException e) {
            log.error("Error walking bag stage", e);
        }
    }

    private boolean matchTar(Path path, BasicFileAttributes attr) {
        // should check the mime type instead of ends with
        log.trace("testing {}", path);
        return !attr.isDirectory() && path.toString().endsWith(".tar");
    }

    @VisibleForTesting
    void dpn(Path tarchive) {
        Path name = tarchive.getFileName();
        Path parent = tarchive.getParent().getFileName();

        String uuid = name.toString().substring(0, name.toString().lastIndexOf(".tar"));
        log.info("Checking {}/{} replication status", parent, uuid);
        // TODO: Get snapshot based on alt id (no support in bridge yet)
        // TODO: Could use enqueue instead of execute
        Call<Bag> call = registry.getBag(uuid);
        try {
            Response<Bag> response = call.execute();
            if (response.isSuccessful()) {
                Bag body = response.body();
                if (body.getReplicatingNodes().size() == 3) {
                    log.info("REMOVING {} at {}", uuid, tarchive);
                    if (!settings.isCleanDryRun()) {
                        Files.delete(tarchive);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Error communicating with registry", e);
        }
    }


    @Scheduled(cron = "0 0 0 * * *")
    public void cleanChron() {
        String bagStage = settings.getChron().getBags();
        Path bags = Paths.get(bagStage);

        try (Stream<Path> files =
                     Files.find(bags, 2, (p, a) -> a.isDirectory())) {
            files.filter(this::isSnapshot)
                    .forEach(this::chron);
        } catch (IOException e) {
            log.warn("Error walking bag stage", e);
        }

    }

    @VisibleForTesting
    void chron(Path path) {
        Path name = path.getFileName();
        Path parent = path.getParent().getFileName();

        log.info("Checking {}/{} replication status", parent, name);

        Call<PageImpl<org.chronopolis.rest.models.Bag>> bags = ingest.getBags(ImmutableMap.of(
                "name", name.toString(),
                "depositor", parent.toString()));
        try {
            Response<PageImpl<org.chronopolis.rest.models.Bag>> response = bags.execute();
            if (response.isSuccessful()) {
                PageImpl<org.chronopolis.rest.models.Bag> body = response.body();
                if (body.getTotalElements() == 0 || body.getTotalElements() > 1) {
                    log.warn("Multiple bags found, aborting");
                } else {
                    org.chronopolis.rest.models.Bag bag = body.getContent().get(0);
                    if (bag.getStatus() == BagStatus.PRESERVED) {
                        log.info("REMOVING {} at {}", name, path);
                        if (!settings.isCleanDryRun()) {
                            Files.deleteIfExists(path);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Error communicating with ingest", e);
        }
    }

    @VisibleForTesting
    void cleanDirectory(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.error("Error removing directory {}", path, e);
        }
    }

    /**
     * Test if a directory is from a snapshot
     *
     * @param root the root of the bag
     * @return true if the bag was made from a snapshot, false otherwise
     */
    @VisibleForTesting
    boolean isSnapshot(Path root) {
        return Files.exists(root.resolve(SNAPSHOT_FILE));
    }

    /**
     * Test is a tarball is a serialized bag which was created
     * from a snapshot
     *
     * @param tarball the tarball to check
     * @return true if the bag was made from a snapshot and is a tarball, false otherwise
     */
    @VisibleForTesting
    boolean isSerializedSnapshot(Path tarball) {
        boolean hasSnapshotFile = false;
        try {
            String bag = tarball.getFileName().toString();
            String tarEntry = bag.substring(0, bag.lastIndexOf(".")) + "/" + SNAPSHOT_FILE;

            TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(tarball));
            TarArchiveEntry entry;
            // Loop while there are entries and we haven't encountered SNAPSHOT_FILE
            while ((entry = is.getNextTarEntry()) != null && !hasSnapshotFile) {
                hasSnapshotFile = entry.getName().equals(tarEntry);
            }
        } catch (IOException e) {
            log.error("Error opening tarball", e);
        }

        return hasSnapshotFile;
    }

}
