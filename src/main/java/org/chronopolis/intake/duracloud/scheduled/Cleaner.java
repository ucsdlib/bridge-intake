package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableMap;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class to handle scheduled checks of the staging area and determining
 * what to clean up
 *
 * Created by shake on 10/27/16.
 */
@Component
@EnableScheduling
public class Cleaner {
    private final String TAR_TYPE = "application/x-tar";
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
        try {
            Files.walk(bags)
                 .filter(p -> (p.getNameCount() - bags.getNameCount()) == 2)
                 .filter(p -> p.toString().endsWith(".tar")) // should check the mime type instead but that throws an exception :/
                 .forEach(this::dpn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
                    Files.delete(tarchive);
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

        try {
            Files.walk(bags)
                    .filter(p -> (p.getNameCount() - bags.getNameCount()) == 2)
                    .filter(p -> Files.isDirectory(p))
                    .forEach(this::chron);
        } catch (IOException e) {
            log.warn("Error walking bag stage", e);
        }

    }

    private void chron(Path path) {
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
                        Files.delete(path);
                    }
                }


            }

        } catch (IOException e) {
            log.warn("Error communicating with ingest", e);
        }
    }

}
