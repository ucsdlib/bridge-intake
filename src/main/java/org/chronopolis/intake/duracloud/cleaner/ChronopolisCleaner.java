package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.rest.api.DepositorAPI;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.chronopolis.rest.models.storage.ActiveToggle;
import org.chronopolis.rest.models.storage.StagingStorageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Class to remove data under a directory for Chronopolis content. Once data has been removed,
 * the StagingStorageModel of the Bag will be deactivated in the Ingest Server.
 * <p>
 * todo: might want to use the relative path given by the ingest server when  issuing the rm. while
 * everything should be the same, in the event of a schema change later down the line for storing
 * bags, it would make less work. If we do this there's some extra logic in checking that the
 * bagStorage is not null but that should be pretty easy. Maybe targeted for 2.1.1.
 *
 * @author shake
 */
public class ChronopolisCleaner extends Cleaner {

    private final Logger log = LoggerFactory.getLogger(ChronopolisCleaner.class);

    private final DepositorAPI depositors;
    private final StagingService stagingService;
    private final BagStagingProperties properties;

    /**
     * Name of the bag
     */
    private String name;

    /**
     * Name of the depositor
     */
    private String depositor;

    /**
     * The bag
     */
    private Bag bag;

    /**
     * Create a ChronopolisCleaner which will attempt to remove a bag from
     * staging.
     */
    public ChronopolisCleaner(Path relative,
                              DepositorAPI depositors,
                              StagingService stagingService,
                              BagStagingProperties properties,
                              Bag bag) {
        super(relative, properties);
        this.depositors = depositors;
        this.stagingService = stagingService;
        this.properties = properties;
        this.bag = bag;
    }

    /**
     * Create a ChronopolisCleaner which will attempt to clean a bag from
     * staging given its name and depositor. A query  will be executed in order
     * to retrieve the bag from the Ingest Server in order to validate that the
     * bag can be removed.
     */
    public ChronopolisCleaner(Path relative,
                              DepositorAPI depositors,
                              StagingService stagingService,
                              BagStagingProperties properties,
                              String depositor,
                              String name) {
        super(relative, properties);
        this.depositors = depositors;
        this.stagingService = stagingService;
        if (name == null || depositor == null) {
            throw new IllegalArgumentException("Depositor and Bag Name are not allowed to be null");
        }


        this.bag = null;
        this.name = name;
        this.depositor = depositor;
        this.properties = properties;
    }

    @Override
    public Boolean call() {
        Optional<Bag> option = Optional.ofNullable(this.bag);

        return option.map(this::fromBag)
                .orElseGet(this::fromQuery);
    }

    private boolean fromBag(Bag bag) {
        log.info("[Cleaner] Removing content for {} {}", bag.getDepositor(), bag.getName());
        boolean success = false;
        Path root = Paths.get(properties.getPosix().getPath());
        Path full = root.resolve(bag.getDepositor()).resolve(bag.getName());

        if (bag.getStatus() == BagStatus.PRESERVED) {
            success = rm(full) && deactivate(bag);
        }

        return success;
    }

    private boolean fromQuery() {
        if (depositor == null && name == null) {
            throw new IllegalArgumentException("Depositor and Bag Name are not allowed to be null");
        }

        log.info("[Cleaner] Removing content for {} {}", depositor, name);
        // create the full path to the bag for use later
        final Path root = Paths.get(properties.getPosix().getPath());
        final Path full = root.resolve(depositor).resolve(name);

        // retrieve the bag
        SimpleCallback<Bag> callback = new SimpleCallback<>();
        Call<Bag> call = depositors.getDepositorBag(depositor, name);
        call.enqueue(callback);

        return callback.getResponse()
                .filter(this::checkPreserved) // only continue if the bag is preserved
                .map(bag -> rm(full) && deactivate(bag)).orElse(false);
    }

    private boolean checkPreserved(Bag bag) {
        boolean preserved = bag.getStatus() == BagStatus.PRESERVED;
        if (!preserved) {
            log.warn("[Cleaner] Unable to continue, status is {}", bag.getStatus());
        }
        return preserved;
    }

    /**
     * Deactivate the bag storage for a given bag
     *
     * @param bag the bag to deactivate storage for
     */
    private boolean deactivate(Bag bag) {
        boolean deactivated = true;
        if (bag.getBagStorage() != null) {
            log.info("[Cleaner] Deactivating storage for {} {}", bag.getDepositor(), bag.getName());
            SimpleCallback<StagingStorageModel> stagingCB = new SimpleCallback<>();
            Call<StagingStorageModel> call = stagingService
                    .toggleStorage(bag.getId(), "BAG", new ActiveToggle(false));
            call.enqueue(stagingCB);
            deactivated = stagingCB.getResponse()
                    .map(staging -> !staging.isActive()) // make sure the staging model is NOT active
                    .orElse(false);
        }

        return deactivated;
    }
}
