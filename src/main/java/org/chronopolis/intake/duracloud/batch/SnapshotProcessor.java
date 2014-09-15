package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.digest.Digest;
import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shake on 7/29/14.
 */
public class SnapshotProcessor implements ItemProcessor<BagModel, BagModel> {
    private final Logger log = LoggerFactory.getLogger(SnapshotProcessor.class);
    public static final String MANIFEST_NAME = "manifest-sha256.txt";

    private final IntakeSettings settings;
    // private StatusRepository repository;

    public SnapshotProcessor(IntakeSettings settings) {
        this.settings = settings;
    }

    @Override
    public BagModel process(final BagModel bagModel) throws Exception {
        String bagId = bagModel.getBagId();
        // Status status = repository.findById(bagId);

        try {
            // Will want these to come from the sha256 file given by duraspace
            // Path manifestPath = bagModel.getChronPackage()
            String duracloudBase = settings.getDuracloudSnapshotStage();
            Path manifestPath = Paths.get(duracloudBase, bagId, MANIFEST_NAME);

            log.info("Loading digests from {}", manifestPath);
            Map<String, String> digests = readManifest(manifestPath);

            // Add digests to bag
            bagModel.registerDigests(digests);
        }
        // TODO: How to properly handle errors (return null vs something else)
        catch (IOException ex) {
            // setStatusError(status);
            return null;
        }

        return bagModel;
    }

    private Map<String, String> readManifest(Path manifest) throws IOException {
        Map<String, String> digestMap = new HashMap<>();
        try (BufferedReader br = Files.newBufferedReader(manifest, Charset.defaultCharset())) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+", 2);
                if (parts.length != 2) {
                    throw new IOException("Error in manifest! Offending line: " + line);
                }
                String digest = parts[0];
                String path = parts[1];
                digestMap.put(digest.trim(), path.trim());
                log.trace("Captured " + parts[0] + " for " + parts[1]);
            }
        }
        return digestMap;
    }

    Map<String, String> readManifestWithValidate(Path manifest,
                                                 String digest,
                                                 Digest algorithm) {
        return null;
    }

    /**
     * Set bagging status to BAGGING_INTERNAL_ERROR
     *
     * @param status
    private void setStatusInternalError(BagStatus status) {
        status.setBaggingInternalError();
        accessor.put(status);
    }

     * Set bagging status to BAGGING_ERROR
     *
     * @param status
    private void setStatusError(BagStatus status) {
        status.setBaggingError();
        accessor.put(status);
    }
    */


}
