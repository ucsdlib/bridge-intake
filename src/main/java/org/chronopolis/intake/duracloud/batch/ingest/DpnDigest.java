package org.chronopolis.intake.duracloud.batch.ingest;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.function.Function;

/**
 * Register the digest (fixity) for a given bag with the DPN Registry
 *
 * @author shake
 */
public class DpnDigest implements Function<Bag, Bag> {

    private final Logger log = LoggerFactory.getLogger(DpnDigest.class);

    private final BagReceipt receipt;
    private final BalustradeBag bags;
    private final IntakeSettings settings;

    public DpnDigest(BagReceipt receipt, BalustradeBag bags, IntakeSettings settings) {
        this.receipt = receipt;
        this.bags = bags;
        this.settings = settings;
    }

    @Override
    public Bag apply(Bag bag) {
        final String uuid = bag.getUuid();
        final String algorithm = "sha256";

        Digest digest = new Digest()
                .setBag(uuid)
                .setAlgorithm(algorithm)
                .setValue(receipt.getReceipt())
                .setNode(settings.getDpn().getUsername())
                .setCreatedAt(ZonedDateTime.now());

        Call<Digest> call = bags.createDigest(uuid, digest);

        try {
            Response<Digest> response = call.execute();

            // hmm... better ways to do this I'm sure of it
            if (response.isSuccessful() || response.code() == 409) {
                log.info("[{}] Registered digest", uuid);
            } else {
                throw new RuntimeException("Unable to register digest");
            }
        } catch (IOException e) {
            log.error("[{}] Unable to register digest", uuid);
            throw new RuntimeException("Unable to register digest");
        }

        return bag;
    }

}
