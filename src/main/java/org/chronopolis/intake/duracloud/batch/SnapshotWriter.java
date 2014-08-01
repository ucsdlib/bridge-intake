package org.chronopolis.intake.duracloud.batch;

import org.apache.log4j.Logger;
import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.amqp.RoutingKey;
import org.chronopolis.common.digest.Digest;
import org.chronopolis.common.settings.ChronopolisSettings;
import org.chronopolis.db.intake.StatusRepository;
import org.chronopolis.db.intake.model.Status;
import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.ingest.bagger.BagType;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.bagger.exception.BaggingException;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.DirectoryBagBuildListener;
import org.chronopolis.ingest.pkg.DpnBagWriter;
import org.chronopolis.ingest.pkg.ManifestBuildListener;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.TarBagBuildListener;
import org.chronopolis.ingest.pkg.Writer;
import org.chronopolis.messaging.factory.MessageFactory;
import org.chronopolis.messaging.pkg.PackageReadyMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Created by shake on 7/29/14.
 */
public class SnapshotWriter {
    private final Logger log = Logger.getLogger(org.chronopolis.intake.duracloud.batch.SnapshotWriter.class);

    private StatusRepository statusRepository;
    private final ChronopolisSettings settings;
    private final ChronProducer producer;
    private final MessageFactory messageFactory;
    // private final Registry registry;

    public SnapshotWriter(ChronProducer producer,
                          MessageFactory messageFactory,
                          ChronopolisSettings settings) {
                          // Registry registry) {
        this.producer = producer;
        this.messageFactory = messageFactory;
        this.settings = settings;
        // this.registry = registry;
    }

    public void write(BagModel model) {
        final ChronPackage workingPackage = model.getChronPackage();
        // RegistryService registryService = new RegistryServiceImpl();
        Status bagStatus = statusRepository.findById(model.getBagId());
        Path base = Paths.get(settings.getBagStage());

        final ManifestBuilder builder = new ManifestBuilder(workingPackage,
                0);
        OutputStream os = null;
        final boolean isLocal = (model.getIngestionType() == IngestionType.LOCAL ||
                model.getIngestionType() == IngestionType.DPN);

        Writer bagWriter;
        // Set the bag writer and root level directory name of the bag
        if (model.getIngestionType() == IngestionType.DPN) {
            String uuid = UUID.randomUUID().toString();
            bagWriter = new DpnBagWriter(base, workingPackage, uuid);
            // Only use the workingpackage name for now, later we will use the dpn object id
            workingPackage.setSaveName(workingPackage.getName());
        } else {
            bagWriter = new Writer(base, workingPackage);
            workingPackage.setSaveName(workingPackage.getName());
        }

        if (isLocal) {
            // Sort of janky, should probably fix
            if (model.getCompression()) {
                model.setSaveFile(base.resolve(workingPackage.getSaveName() + ".tar").toFile());
                File f = model.getSaveFile();
                if (!f.getParentFile().exists()) {
                    f.getParentFile().mkdirs();
                }
                log.trace(f.getAbsolutePath());

                try {
                    os = new FileOutputStream(f);
                } catch (IOException ioe) {
                    log.error("Error making output stream for bag", ioe);
                    // setStatusError(bagStatus);
                }
            } else {
                model.setSaveFile(base.resolve(workingPackage.getSaveName()).toFile());
            }
        } else {
            // setStatusError(bagStatus);
        }

        boolean isHoley = (model.getBagType() == BagType.HOLEY);

            /*
            writer.setCloseOutput(true);
            if (isHoley) {
                writer.setUrlPattern(model.getUrlPattern());
            }
            */

        ManifestBuildListener.Adapter listener =
                (model.getCompression())
                        ? new TarBagBuildListener(os, base, isHoley, bagWriter)
                        : new DirectoryBagBuildListener(base, workingPackage.getSaveName(), bagWriter);


        builder.getBuildListeners().add(listener);

        try {
            // On a long bag this may take some time, we could move this into
            // a separate process and return a 200. Status can be checked through
            // the Status controller.
            // Or we can just have this be a long poll - TBD w/ duraspace folks
            builder.scanPackage();
        } catch (IOException ex) {
            // setStatusError(bagStatus);
        } catch (BaggingException e) {
            // setStatusInvalidDigests(bagStatus, e.getDigests());
        }

        // registryService.addBagToRegistry(registry, model);
        // setStatusComplete(bagStatus);
        sendToChronopolis(model);
    }


    private void sendToChronopolis(BagModel model) {
        ChronPackage chronPackage = model.getChronPackage();
        String depositor = chronPackage.getDepositor();
        String messageDigest = chronPackage.getMessageDigest();
        String name = chronPackage.getName();

        ChronPackage.Statistics stats = chronPackage.getStats();
        long size = stats.getSize();

        Digest digest = Digest.fromString(messageDigest);
        Path saveFile = model.getSaveFile().toPath();

        PackageReadyMessage packageReadyMessage = messageFactory.packageReadyMessage(
                depositor,
                digest,
                saveFile.getFileName().toString(), // We only want the relative path
                name,                              // (ingest may have a different mount)
                (int) size
        );

        producer.send(packageReadyMessage, RoutingKey.INGEST_BROADCAST.asRoute());
    }


    // TODO: Move status helpers to Status Accessor

    /**
     * Set bagging status to BAGGING_CHECKSUM_ERROR
     *
     * @param status
     * @param invalidDigests invalid digests found while bagging
    private void setStatusInvalidDigests(BagStatus status,
                                         Set<ManifestTuple> invalidDigests) {
        Gson mapper = new Gson();
        status.setBaggingChecksumError(mapper.toJson(invalidDigests));

        statusAccessor.put(status);
    }

     * Set bagging status to BAGGING_COMPLETE
     *
     * @param status
    private void setStatusComplete(BagStatus status) {
        status.setBaggingComplete();
        statusAccessor.put(status);
    }

     * Set bagging status to BAGGING_INTERNAL_ERROR
     *
     * @param status
    private void setStatusInternalError(BagStatus status) {
        status.setBaggingInternalError();
        statusAccessor.put(status);
    }

     * Set bagging status to BAGGING_ERROR
     *
     * @param status
    private void setStatusError(BagStatus status) {
        status.setBaggingError();
        statusAccessor.put(status);
    }
     */


}
