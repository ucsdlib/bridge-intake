package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.rest.api.DepositorAPI;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class to create different types of {@link Cleaner}s
 *
 * @author shake
 */
public class Bicarbonate {

    private final DepositorAPI depositors;
    private final StagingService stagingService;
    private final BagStagingProperties stagingProperties;

    public Bicarbonate(DepositorAPI depositors,
                       StagingService stagingService,
                       BagStagingProperties stagingProperties) {
        this.depositors = depositors;
        this.stagingService = stagingService;
        this.stagingProperties = stagingProperties;
    }

    /**
     * Create a {@link Cleaner} to remove a directory from the staging area
     *
     * @param relative the relative path to remove
     * @return the Cleaner
     */
    public Cleaner cleaner(Path relative) {
        return new Cleaner(relative, stagingProperties);
    }

    /**
     * Create a {@link Cleaner} for removing a bag from Chronopolis staging with a given Bag
     *
     * @param bag the bag to remove
     * @return the ChronopolisCleaner
     */
    public Cleaner forChronopolis(Bag bag) {
        // Note: the bag staging may not exist, so use the depositor and bag name to create the path
        Path relative = Paths.get(bag.getDepositor(), bag.getName());
        return new ChronopolisCleaner(relative, depositors, stagingService, stagingProperties, bag);
    }

    /**
     * Create a {@link Cleaner} for removing a bag from Chronopolis staging given the names of the
     * depositor and bag
     *
     * @param depositor the name of the depositor
     * @param bag       the name of the bag
     * @return the ChronopolisCleaner
     */
    public Cleaner forChronopolis(String depositor, String bag) {
        Path relative = Paths.get(depositor, bag);
        return new ChronopolisCleaner(relative, depositors, stagingService,
                stagingProperties, depositor, bag);
    }

}
