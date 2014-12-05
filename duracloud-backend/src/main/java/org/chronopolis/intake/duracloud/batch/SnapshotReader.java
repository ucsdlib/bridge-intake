package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.db.intake.StatusRepository;
import org.chronopolis.db.intake.model.Status;
import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.ingest.bagger.BagType;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagDirectoryFilter;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by shake on 7/29/14.
 */
public class SnapshotReader implements ItemReader<BagModel> {
    private final Logger log = LoggerFactory.getLogger(SnapshotReader.class);

    private final List<DuracloudRequest> requests;
    private final IntakeSettings intakeSettings;
    private final DirectoryStream.Filter<Path> filter;
    private StatusRepository statusRepository;

    public SnapshotReader(DuracloudRequest bag,
                          IntakeSettings intakeSettings,
                          StatusRepository statusRepository) {
        this.requests = new ArrayList<>();
        this.requests.add(bag);
        this.intakeSettings = intakeSettings;
        this.statusRepository = statusRepository;
        this.filter = new BagDirectoryFilter();
    }

    @Override
    public BagModel read() {
        // Required to be a proper ItemReader
        if (requests.isEmpty()) {
            return null;
        }

        DuracloudRequest request = requests.remove(0);

        String base = intakeSettings.getDuracloudSnapshotStage();
        String bagId = request.getSnapshotID();
        String depositor = request.getDepositor();
        String collectionName = request.getCollectionName();
        ChronPackage pkg = new ChronPackage();

        Set<File> tagFiles = new HashSet<>();

        Path snapshot = Paths.get(base, bagId);
        Path data = snapshot.resolve("data");

        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(snapshot, filter)) {
            for (Path p : stream) {
                System.out.println("Adding tag file " + p.toString());
                log.trace("Adding tag file " + p.toString());
                pkg.addTagFile(p.toFile());
            }
        } catch (IOException e) {
            log.error("IOException", e);
        }

        pkg.setName(collectionName);
        pkg.setDepositor(depositor);
        pkg.getRootList().put(data.toFile(), true);
        pkg.addTagFiles(tagFiles);
        pkg.setProvidedManifest(true);

        BagModel model = createBagModel(bagId,
                pkg,
                BagType.FILLED,
                IngestionType.DPN,
                false);

        // TODO: How to store the bag model (if we want to?)
        Status bagStatus = new Status(bagId, depositor, collectionName);
        statusRepository.save(bagStatus);

        return model;
    }

    /**
     * Create a new BagModel object
     *
     * @param pkg
     * @param bagType
     * @param ingestionType
     * @param compressed
     * @return the BagModel
     */
    private BagModel createBagModel(String bagId,
                                    ChronPackage pkg,
                                    BagType bagType,
                                    IngestionType ingestionType,
                                    Boolean compressed) {
        BagModel model = new BagModel();
        model.setBagId(bagId);
        model.setChronPackage(pkg);
        model.setBagType(bagType);
        model.setIngestionType(ingestionType);
        model.setCompression(compressed);
        return model;
    }

}