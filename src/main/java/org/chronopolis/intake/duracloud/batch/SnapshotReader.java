package org.chronopolis.intake.duracloud.batch;

import org.apache.log4j.Logger;
import org.chronopolis.db.intake.StatusRepository;
import org.chronopolis.db.intake.model.Status;
import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.ingest.bagger.BagType;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagDirectoryFilter;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by shake on 7/29/14.
 */
public class SnapshotReader {
    private final Logger log = Logger.getLogger(org.chronopolis.intake.duracloud.batch.SnapshotReader.class);

    private final DuracloudRequest bag;
    private final IntakeSettings intakeSettings;
    private final DirectoryStream.Filter<Path> filter;
    // private StatusRepository statusRepository;

    public SnapshotReader(DuracloudRequest bag, final IntakeSettings intakeSettings) {
        this.bag = bag;
        this.intakeSettings = intakeSettings;
        this.filter = new BagDirectoryFilter();
    }

    public BagModel read() {
        String base = intakeSettings.getDuracloudStage();
        String bagId = bag.getSnapshotID();
        String depositor = bag.getDepositor();
        String collectionName = bag.getCollectionName();
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
            //return serverError("Error creating tag files");
        }

        pkg.setName(collectionName);
        pkg.setDepositor(depositor);
        pkg.getRootList().put(data.toFile(), true);
        pkg.addTagFiles(tagFiles);
        pkg.setProvidedManifest(true);

        Status status = new Status(bagId, depositor, collectionName);
        BagModel model = createBagModel(bagId,
                pkg,
                BagType.FILLED,
                IngestionType.DPN,
                false);


        // TODO: How to store the bag model
        // models.put(bagId, model);
        System.out.print("Saving status to repo");
        // statusRepository.save(status);
        System.out.println("...done");

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
