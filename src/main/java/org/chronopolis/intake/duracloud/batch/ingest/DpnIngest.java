package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * First step in DPN Ingestion
 * <p>
 * Get the Bag from
 *
 * @author shake
 */
public class DpnIngest implements Supplier<Bag> {

    /**
     * static factory for testing
     *
     * todo: we might be able to get rid of this and simplify testing by passing in the DpnInfoReader
     */
    static class ReaderFactory {
        DpnInfoReader reader(Path save, String name) throws IOException {
            try (TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(save))) {
                return DpnInfoReader.read(is, name);
            }
        }
    }

    private final Logger log = LoggerFactory.getLogger(DpnIngest.class);

    private final BagData data;
    private final BagReceipt receipt;
    private final BalustradeBag bags;
    private final IntakeSettings settings;
    private final ReaderFactory readerFactory;
    private final BagStagingProperties staging;

    public DpnIngest(BagData data,
                     BagReceipt receipt,
                     BalustradeBag bags,
                     IntakeSettings settings,
                     BagStagingProperties staging) {
        this.data = data;
        this.receipt = receipt;
        this.bags = bags;
        this.settings = settings;
        this.readerFactory = new ReaderFactory();
        this.staging = staging;
    }

    @VisibleForTesting
    public DpnIngest(BagData data,
                     BagReceipt receipt,
                     BalustradeBag bags,
                     IntakeSettings settings,
                     BagStagingProperties staging,
                     ReaderFactory readerFactory) {
        this.data = data;
        this.receipt = receipt;
        this.bags = bags;
        this.settings = settings;
        this.readerFactory = readerFactory;
        this.staging = staging;
    }

    /**
     * Get or Create a Bag from the dpn registry
     *
     * @return the bag to get
     */
    @Override
    public Bag get() {
        SimpleCallback<Bag> cb = new SimpleCallback<>();

        log.debug("[{}] Seeing if bag is already registered for receipt", receipt.getName());
        Call<Bag> call = bags.getBag(receipt.getName());
        call.enqueue(cb);

        Optional<Bag> response = cb.getResponse();

        return response.orElseGet(this::register);
    }

    /**
     * Create a bag in the dpn registry
     *
     * @return the bag created
     */
    private Bag register() {
        log.info("[{}] Creating bag in DPN Registry", receipt.getName());
        Posix posix = staging.getPosix();
        Optional<Bag> registeredBag = Optional.empty();
        SimpleCallback<Bag> cb = new SimpleCallback<>();

        final char type = 'D';
        final String name = receipt.getName();
        final String depositor = data.depositor();
        final Path save = Paths.get(posix.getPath(), depositor, name + ".tar");

        try {
            DpnInfoReader reader = readerFactory.reader(save, name);

            Bag bag = new Bag();
            bag.setAdminNode("chron")
                    .setUuid(name)
                    .setBagType(type)
                    .setMember(data.member())
                    .setCreatedAt(ZonedDateTime.now())
                    .setUpdatedAt(ZonedDateTime.now())
                    .setSize(save.toFile().length())
                    .setLocalId(reader.getLocalId())
                    .setRights(reader.getRightsIds())
                    .setVersion(reader.getVersionNumber())
                    .setIngestNode(settings.getDpn().getUsername())
                    .setInterpretive(reader.getInterpretiveIds())
                    .setFirstVersionUuid(reader.getFirstVersionUUID())
                    .setReplicatingNodes(ImmutableList.of("chron"));

            Call<Bag> call = bags.createBag(bag);
            call.enqueue(cb);
            registeredBag = cb.getResponse();
        } catch (IOException exception) {
            log.error("[{}] Unable to register with DPN Registry", name, exception);
        }

        // I really don't like this exception, but I also don't like the supplier returning an
        // optional... maybe it could work I'm not really sure at the moment what the best way
        // to go about everything is
        return registeredBag.orElseThrow(() -> new RuntimeException("Unable to get bag"));
    }
}
