package org.chronopolis.intake.duracloud.batch.support;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;
import org.chronopolis.bag.packager.Packager;
import org.chronopolis.bag.packager.PackagerData;
import org.chronopolis.bag.writer.WriteJob;
import org.chronopolis.bag.writer.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Updated write job to write BagIt metadata files first
 *
 * Created by shake on 1/4/17.
 */
public class TarWriteJob extends WriteJob {
    private final Logger log = LoggerFactory.getLogger(TarWriteJob.class);

    private final Bag bag;
    private final boolean validate;
    private final Packager packager;
    private WriteResult result;

    public TarWriteJob(Bag bag, boolean validate, Packager packager) {
        super(bag, validate, packager);
        this.bag = bag;
        this.validate = validate;
        this.packager = packager;
        this.result = new WriteResult();
    }

    public WriteResult call() {
        result.setBag(bag);
        bag.prepareForWrite();

        HashFunction hash = bag.getManifest().getDigest().getHashFunction();

        log.info("Starting build for {}", bag.getName());
        PackagerData data = null;
        try {
            data = packager.startBuild(bag.getName());
            TagManifest tagManifest = bag.getTagManifest();
            writeManifest(bag, hash, tagManifest, data);
            writeTagFiles(bag, hash, tagManifest, data);
            writeTagManifest(bag, hash, tagManifest, data);
            writePayloadFiles(bag, hash, data);
        } catch (Exception e) {
            result.setSuccess(false);
            log.error("Error building bag", e);
        } finally {
            packager.finishBuild(data);
        }

        return result;
    }

    // These will eventually be replaced when we update our bagging library

    private void writeTagManifest(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) throws IOException {
        HashCode hashCode;

        // Write the tagmanifest
        log.info("Writing tagmanifest for {}", bag.getName());
        hashCode = packager.writeManifest(tagManifest, hash, data);
        String receipt = hashCode.toString();
        bag.setReceipt(receipt);
        result.setReceipt(receipt);
        log.debug("HashCode is {}", receipt);
    }

    private void writeTagFiles(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) throws IOException {
        HashCode hashCode;

        // Write tag files
        log.info("Writing tag files for {}", bag.getName());
        for (TagFile tag : bag.getTags().values()) {
            log.debug("{}", tag.getPath());
            hashCode = packager.writeTagFile(tag, hash, data);
            tagManifest.addTagFile(tag.getPath(), hashCode);
            log.debug("HashCode is {}", hashCode.toString());
        }
    }

    private void writeManifest(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) throws IOException {
        HashCode hashCode;

        // Write manifest
        log.info("Writing manifest for {}", bag.getName());
        Manifest manifest = bag.getManifest();
        hashCode = packager.writeManifest(manifest, hash, data);
        tagManifest.addTagFile(manifest.getPath(), hashCode);
        log.debug("HashCode is {}", hashCode.toString());
    }

    private void writePayloadFiles(Bag bag, HashFunction hash, PackagerData data) throws IOException {
        HashCode hashCode;

        // Write payload files
        // Validate if wanted
        log.info("Writing payload files for {}", bag.getName());
        if (bag.getFiles().isEmpty()) {
            log.warn("Bag has no payload files, marking as error");
            bag.addBuildError("Bag has no payload files");
        }

        for (PayloadFile payloadFile : bag.getFiles().values()) {
            log.trace(payloadFile.getFile() + ": ");
            hashCode = packager.writePayloadFile(payloadFile, hash, data);
            log.trace(hashCode.toString());

            if (validate && !hashCode.equals(payloadFile.getDigest())) {
                log.error("Digest mismatch for file {}. Expected {}; Found {}",
                        payloadFile, payloadFile.getDigest(), hashCode);
                result.setSuccess(false);
                bag.addError(payloadFile);
            }

            bag.addFile(payloadFile);
        }
    }

}
