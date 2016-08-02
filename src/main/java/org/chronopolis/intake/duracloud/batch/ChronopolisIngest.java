package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.IngestRequest;
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

    private BagData data;
    private List<BagReceipt> receipts;

    private IngestAPI chron;

    public ChronopolisIngest(BagData data,
                             List<BagReceipt> receipts,
                             IngestAPI ingest,
                             IntakeSettings settings) {
        this.data = data;
        this.chron = ingest;
        this.receipts = receipts;
        this.settings = settings;
    }

    @Override
    public void run() {
        receipts.forEach(this::chronopolis);
    }

    private BagReceipt chronopolis(BagReceipt receipt) {
        String depositor = data.depositor();

        log.info("Notifying chronopolis about bag {}", receipt.getName());
        Path location = Paths.get(settings.getBagStage(), depositor, receipt.getName() + ".tar");

        IngestRequest chronRequest = new IngestRequest();
        chronRequest.setRequiredReplications(1);
        chronRequest.setName(receipt.getName());
        chronRequest.setDepositor(depositor);
        chronRequest.setLocation(location.toString()); // This is the relative path

        chronRequest.setReplicatingNodes(
                ImmutableList.of(settings.getChronReplicationNodes()));

        Call<Bag> stageCall = chron.stageBag(chronRequest);
        try {
            retrofit2.Response<Bag> response = stageCall.execute();
            if (response.isSuccess()) {
                log.info("Registered bag with chronopolis. {}: {}", response.code(), response.body());
            } else {
                log.warn("Error registering bag. {}: {}", response.code(), response.errorBody().string());
            }
        } catch (IOException e) {
            log.error("Unable to stage bag with chronopolis", e);
        }

        return receipt;
    }
}
