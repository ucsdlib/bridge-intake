package org.chronopolis.intake.duracloud.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Chron;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.IngestRequest;
import org.chronopolis.rest.service.IngestRequestSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


/**
 * Ingest a bag into Chronopolis
 *
 * Created by shake on 5/31/16.
 */
public class ChronopolisIngest implements Runnable {
    private final Logger log = LoggerFactory.getLogger(ChronopolisIngest.class);

    private IntakeSettings settings;
    private IngestSupplierFactory factory;
    private BagStagingProperties stagingProperties;

    private BagData data;
    private List<BagReceipt> receipts;

    private IngestAPI chron;

    public ChronopolisIngest(BagData data,
                             List<BagReceipt> receipts,
                             IngestAPI ingest,
                             IntakeSettings settings,
                             BagStagingProperties stagingProperties) {
        this(data, receipts, ingest, settings, stagingProperties, new IngestSupplierFactory());
    }

    @VisibleForTesting
    protected ChronopolisIngest(BagData data,
                             List<BagReceipt> receipts,
                             IngestAPI ingest,
                             IntakeSettings settings,
                             BagStagingProperties stagingProperties,
                             IngestSupplierFactory supplierFactory) {
        this.data = data;
        this.chron = ingest;
        this.receipts = receipts;
        this.settings = settings;
        this.stagingProperties = stagingProperties;
        this.factory = supplierFactory;
    }

    @Override
    public void run() {
        if (settings.pushChronopolis()) {
            receipts.forEach(this::chronopolis);
        }
    }

    private BagReceipt chronopolis(BagReceipt receipt) {
        Chron chronSettings = settings.getChron();
        String prefix = chronSettings.getPrefix();
        String depositor = Strings.isNullOrEmpty(prefix) ? data.depositor() : prefix + data.depositor();

        // I'm sure there's a better way to handle this... but for now this should be ok
        String bag = settings.pushDPN() ? receipt.getName() + ".tar" : receipt.getName();
        Path stage = Paths.get(stagingProperties.getPosix().getPath());
        Path location = stage.resolve(data.depositor()).resolve(bag);

        log.info("[{}] Building ingest request for chronopolis", receipt.getName());
        log.info("[depositor, {}]", depositor);
        IngestRequestSupplier supplier = factory.supplier(location, stage, depositor, receipt.getName());
        supplier.get().ifPresent(this::pushRequest);
        return receipt;
    }

    private void pushRequest(IngestRequest ingestRequest) {
        Chron chronSettings = settings.getChron();
        List<String> replicatingNodes = chronSettings.getReplicatingTo();
        ingestRequest.setStorageRegion(null);
        ingestRequest.setRequiredReplications(replicatingNodes.size());
        ingestRequest.setReplicatingNodes(replicatingNodes);

        Call<Bag> stageCall = chron.stageBag(ingestRequest);
        try {
            retrofit2.Response<Bag> response = stageCall.execute();
            if (response.isSuccessful()) {
                log.info("Registered bag with chronopolis. {}: {}", response.code(), response.body());
            } else {
                log.warn("Error registering bag. {}: {}", response.code(), response.errorBody().string());
            }
        } catch (IOException e) {
            log.error("Unable to stage bag with chronopolis", e);
        }
    }

    /**
     * Delegate class so that we can use a Mock supplier
     */
    public static class IngestSupplierFactory {
        public IngestRequestSupplier supplier(Path location, Path stage, String depositor, String name) {
            return new IngestRequestSupplier(location, stage, depositor, name);
        }
    }
}
