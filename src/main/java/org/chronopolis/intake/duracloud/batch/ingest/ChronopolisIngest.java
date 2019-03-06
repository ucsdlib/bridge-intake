package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.annotations.VisibleForTesting;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.SimpleCallback;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.api.FileService;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.File;
import org.chronopolis.rest.models.Fixity;
import org.chronopolis.rest.models.StagingStorage;
import org.chronopolis.rest.models.create.BagCreate;
import org.chronopolis.rest.models.create.StagingCreate;
import org.chronopolis.rest.models.enums.BagStatus;
import org.chronopolis.rest.models.enums.FixityAlgorithm;
import org.chronopolis.rest.service.BagFileCsvGenerator;
import org.chronopolis.rest.service.IngestRequestSupplier;
import org.slf4j.Logger;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static okhttp3.MultipartBody.Part;


/**
 * Ingest a bag into Chronopolis
 * <p>
 * As we have added more operations to ingesting content, the constructor has continued to grow. It
 * might be good to look into splitting up operations similar to the DPN operations.
 *
 * @author shake
 */
public class ChronopolisIngest implements Runnable {
    private final Logger log;

    private final BridgeContext bridgeContext;
    private final IntakeSettings settings;
    private final IngestSupplierFactory factory;
    private final BagStagingProperties stagingProperties;

    // todo: it might be better to pass these as parameters to run (maybe as a BiFunction?)
    private final BagData data;
    private final List<BagReceipt> receipts;

    // Chronopolis API
    private final BagService bags;
    private final FileService files;
    private final StagingService staging;
    private final DepositorService depositorService;

    public ChronopolisIngest(BagData data,
                             List<BagReceipt> receipts,
                             BagService bags,
                             StagingService staging,
                             IntakeSettings settings,
                             BagStagingProperties stagingProperties,
                             FileService fileService,
                             DepositorService depositorService,
                             BridgeContext bridgeContext) {
        this(data, receipts, bags, fileService, staging, depositorService, settings,
                stagingProperties, bridgeContext, new IngestSupplierFactory());
    }

    @VisibleForTesting
    protected ChronopolisIngest(BagData data,
                                List<BagReceipt> receipts,
                                BagService bags,
                                FileService files,
                                StagingService staging,
                                DepositorService depositors,
                                IntakeSettings settings,
                                BagStagingProperties stagingProperties,
                                BridgeContext bridgeContext,
                                IngestSupplierFactory supplierFactory) {
        this.log = bridgeContext.getLogger();

        this.data = data;
        this.receipts = receipts;
        this.settings = settings;
        this.factory = supplierFactory;
        this.bridgeContext = bridgeContext;
        this.stagingProperties = stagingProperties;

        this.bags = bags;
        this.files = files;
        this.staging = staging;
        this.depositorService = depositors;
    }

    @Override
    public void run() {
        if (bridgeContext.getPush() != Push.NONE) {
            receipts.forEach(this::chronopolis);
        }
    }

    private BagReceipt chronopolis(BagReceipt receipt) {
        String name = receipt.getName();
        String depositor = data.depositor();

        // ok similar to dpn intake, but... a bit more streamlined I guess
        // query the bag
        //   !exists -> deposit
        // bag optional
        //   exists && status == DEPOSITED -> create files
        //   exists && status == INITIALIZED -> create staging
        getBag(depositor, name).ifPresent(bag -> {
            if (bag.getStatus() == BagStatus.DEPOSITED) {
                createFiles(bag);
            } else {
                createStaging(bag);
            }
        });

        return receipt;
    }

    /**
     * Attempt to get a {@link Bag} and if it is not found attempt to register
     *
     * @param depositor the namespace of the depositor who 'owns' the {@link Bag}
     * @param name      the name given to the {@link Bag}
     * @return an Optional encapsulating if the {@link Bag} was found/created
     */
    private Optional<Bag> getBag(String depositor, String name) {
        Call<Bag> call = depositorService.getDepositorBag(depositor, name);
        SimpleCallback<Bag> callback = enqueueCallback(call);
        // must be a better way to handle this...
        if (!callback.getResponse().isPresent()) {
            return deposit(depositor, name);
        }

        return callback.getResponse();
    }

    /**
     * Attempt to create a {@link Bag} in Chronopolis
     *
     * @param depositor the namespace of the Depositor for the Bag
     * @param name      the name for the Bag
     * @return an Optional encapsulating the result of the operation
     */
    private Optional<Bag> deposit(String depositor, String name) {
        log.info("[{}] Building ingest request for chronopolis", name);
        String bag = name + bridgeContext.getPush().getPostfix();
        Path stage = Paths.get(stagingProperties.getPosix().getPath());
        Path location = stage.resolve(data.depositor()).resolve(bag);
        return factory.supplier(location, stage, depositor, name)
                .get().map(bags::deposit)
                .map(this::enqueueCallback)
                .flatMap(SimpleCallback::getResponse);
    }

    /**
     * Just combine some calls so we make things a bit nicer
     *
     * @param call the call to enqueue a callback for
     * @param <T>  the type of the ResponseEntity of the Call
     * @return a freshly generated callback
     */
    private <T> SimpleCallback<T> enqueueCallback(Call<T> call) {
        SimpleCallback<T> callback = new SimpleCallback<>();
        call.enqueue(callback);
        return callback;
    }

    /**
     * Generate and upload a CSV containing the {@link File}s and {@link Fixity} for the given
     * {@link Bag}. This will block while creating the CSV and while uploading the file to the
     * Chronopolis Ingest Server.
     *
     * @param bag the Bag to create files for
     */
    private void createFiles(Bag bag) {
        log.info("[{}] Building csv file ingest request for chronopolis", bag.getName());
        Path stage = Paths.get(stagingProperties.getPosix().getPath());
        Path root = stage.resolve(bag.getDepositor()).resolve(bag.getName());
        Path output = Paths.get(settings.getChron().getWorkDirectory());
        FixityAlgorithm algorithm = FixityAlgorithm.SHA_256;
        factory.generator(output, root, algorithm)
                .call()
                .getCsv()
                .map(csv -> {
                    RequestBody body = RequestBody.create(
                            MediaType.parse("text/csv"),
                            csv.toFile());
                    Part part = Part.createFormData("file", csv.toFile().getName(), body);
                    return files.createBatch(bag.getId(), part);
                }).ifPresent(this::enqueueCallback);

    }

    /**
     * Create a {@link StagingStorage} resource for a {@link Bag} if it does not already have one
     * <p>
     * Since this is the initial distribution, we can use the fields from the {@link Bag} in order
     * to determine the size and number of files which are staged (we passed them earlier during the
     * {@link BagCreate}).
     *
     * @param bag the Bag to create a {@link StagingStorage} resource for
     */
    private void createStaging(Bag bag) {
        Path stage = Paths.get(stagingProperties.getPosix().getPath());
        Path bagPath = stage.resolve(bag.getDepositor()).resolve(bag.getName());

        if (!bagPath.toFile().exists()) {
            log.error("[{}/{}] Unable to find bag in staging area {}!",
                    bag.getDepositor(), bag.getName(), stage);
        } else if (bag.getBagStorage() == null) {
            log.info("[{}] Creating staging resource", bag.getName());
            StagingCreate create = new StagingCreate();
            create.setSize(bag.getSize());
            create.setTotalFiles(bag.getTotalFiles());
            create.setLocation(stage.relativize(bagPath).toString());
            create.setStorageRegion(stagingProperties.getPosix().getId());

            Call<StagingStorage> createCall =
                    staging.createStorageForBag(bag.getId(), "BAG", create);
            enqueueCallback(createCall);
        }
    }

    /**
     * Delegate class so that we can use a Mock supplier
     * <p>
     * Let's try to DI the request supplier instead - need to make it an interface but that's easy
     */
    public static class IngestSupplierFactory {
        public BagFileCsvGenerator generator(Path output, Path root, FixityAlgorithm algorithm) {
            return new BagFileCsvGenerator(output, root, algorithm);
        }

        public IngestRequestSupplier supplier(Path location,
                                              Path stage,
                                              String depositor,
                                              String name) {
            return new IngestRequestSupplier(location, stage, depositor, name);
        }
    }
}
