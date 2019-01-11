package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.batch.check.ChronopolisCheck;
import org.chronopolis.intake.duracloud.batch.ingest.ChronopolisIngest;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.api.FileService;
import org.chronopolis.rest.api.StagingService;

import java.util.List;

/**
 * Same as DpnFactory
 *
 * @author shake
 */
public class ChronFactory {

    private final BagService bags;
    private final FileService files;
    private final StagingService staging;
    private final DepositorService depositors;

    private final Bicarbonate cleaner;
    private final IntakeSettings settings;
    private final BagStagingProperties stagingProperties;

    public ChronFactory(BagService bags,
                        FileService files,
                        StagingService staging,
                        DepositorService depositors,
                        Bicarbonate cleaner,
                        IntakeSettings settings,
                        BagStagingProperties stagingProperties) {
        this.bags = bags;
        this.files = files;
        this.staging = staging;
        this.depositors = depositors;
        this.cleaner = cleaner;
        this.settings = settings;
        this.stagingProperties = stagingProperties;
    }

    public ChronopolisIngest ingest(BagData data,
                                    List<BagReceipt> receipts,
                                    BridgeContext context) {
        return new ChronopolisIngest(data,
                receipts,
                bags,
                staging,
                settings,
                stagingProperties,
                files,
                depositors,
                context);
    }

    public ChronopolisCheck check(BagData data, List<BagReceipt> receipts, BridgeContext context) {
        return new ChronopolisCheck(data, receipts, context, depositors, cleaner);
    }

}
