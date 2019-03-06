package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.models.Depositor;
import org.slf4j.Logger;
import retrofit2.Call;

import java.util.function.BiPredicate;

/**
 * @author shake
 */
public class DepositorCheck implements BiPredicate<BagData, BridgeContext> {

    private final Notifier notifier;
    private final DepositorService chronopolis;

    public DepositorCheck(Notifier notifier, DepositorService chronopolis) {
        this.notifier = notifier;
        this.chronopolis = chronopolis;
    }

    /**
     * Simple validation to ensure that a given depositor exists in Chronopolis and DPN if required
     *
     * @param bagData the BagData containing the name and uuid of the depositor
     * @param context the Bridge which is currently being operated on
     * @return true if the member if found in Chronopolis (and DPN if required)
     */
    @Override
    public boolean test(BagData bagData, BridgeContext context) {
        Logger log = context.getLogger();
        // For verifying that a member exists in chronopolis and/or dpn
        boolean exists = true;
        StringBuilder message = new StringBuilder();
        SimpleCallback<Depositor> chronCallback = new SimpleCallback<>();

        String depositor = bagData.depositor();
        Call<Depositor> chronDepositor = chronopolis.getDepositor(depositor);
        chronDepositor.enqueue(chronCallback);

        message.append("Snapshot Id: ").append(bagData.snapshotId()).append("\n");
        if (!chronCallback.getResponse().isPresent()) {
            exists = false;
            message.append("Chronopolis Depositor ")
                    .append(depositor)
                    .append(" is missing\n");
        }

        if (!exists) {
            log.warn(message.toString());
            notifier.notify("Missing depositor " + depositor, message.toString());
        }

        return exists;
    }
}
