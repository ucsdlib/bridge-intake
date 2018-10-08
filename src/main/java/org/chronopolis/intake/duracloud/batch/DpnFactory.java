package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.check.DpnCheck;
import org.chronopolis.intake.duracloud.batch.ingest.DpnDigest;
import org.chronopolis.intake.duracloud.batch.ingest.DpnIngest;
import org.chronopolis.intake.duracloud.batch.ingest.DpnReplicate;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.rest.api.DepositorService;

import java.util.List;

/**
 * "Factory" class to create {@link DpnDigest}, {@link DpnIngest}, and {@link DpnReplicate}
 *
 * Though I'm not the biggest fan of this type of idea, this is an experiment to see if it can get
 * us through while other decisions are being made about how things should be organized.
 *
 * @author shake
 */
@SuppressWarnings("WeakerAccess")
public class DpnFactory {

    private final LocalAPI dpnApi;
    private final Bicarbonate cleaner;
    private final IntakeSettings settings;
    private final DepositorService depositors;
    private final BagStagingProperties properties;

    public DpnFactory(LocalAPI dpnApi,
                      DepositorService depositors,
                      Bicarbonate cleaner,
                      IntakeSettings settings,
                      BagStagingProperties properties) {
        this.dpnApi = dpnApi;
        this.cleaner = cleaner;
        this.settings = settings;
        this.depositors = depositors;
        this.properties = properties;
    }

    public DpnDigest dpnDigest(BagReceipt receipt, BridgeContext context) {
        return new DpnDigest(receipt, context, dpnApi.getBagAPI(), settings);
    }

    public DpnIngest dpnIngest(BagData data, BagReceipt receipt, BridgeContext context) {
        return new DpnIngest(data, receipt, context, dpnApi.getBagAPI(), settings, properties);
    }

    public DpnReplicate dpnReplicate(String depositor, BridgeContext context) {
        return new DpnReplicate(depositor, context, settings, properties, dpnApi.getTransfersAPI());
    }

    public DpnNodeWeighter dpnNodeWeighter(SnapshotDetails details) {
        return new DpnNodeWeighter(dpnApi.getNodeAPI(), settings, details);
    }

    public DpnCheck dpnCheck(BagData data,
                             List<BagReceipt> receipts,
                             BridgeContext context) {
        return new DpnCheck(data,
                receipts,
                context,
                dpnApi.getBagAPI(),
                dpnApi.getEventsAPI(),
                depositors,
                cleaner,
                settings);
    }
}
