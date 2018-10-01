package org.chronopolis.intake.duracloud.config;

import org.chronopolis.intake.duracloud.config.props.Duracloud.Bridge;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;

/**
 * Contextual information for a Duracloud Bridge so that we know:
 * 1) where the data for this bridge lives
 * 2) where the data for this bridge is going
 * 3) how to communicate with this bridge
 * <p>
 * To keep churn as low as possible, use the same names as from {@link Bridge}
 *
 * @author shake
 */
public class BridgeContext {

    private final BridgeAPI api;

    private Push push;
    private final String manifest;
    private final String restores;
    private final String snapshots;
    private final String chronopolisPrefix;

    public BridgeContext(BridgeAPI api,
                         String chronopolisPrefix,
                         String manifest,
                         String restores,
                         String snapshots,
                         Push push) {
        this.api = api;
        this.push = push;
        this.manifest = manifest;
        this.restores = restores;
        this.snapshots = snapshots;
        this.chronopolisPrefix = chronopolisPrefix;
    }

    /**
     * Get the {@link BridgeAPI} for issuing calls to the Duracloud Bridge API which this context
     * is related to
     *
     * @return the {@link BridgeAPI}
     */
    public BridgeAPI getApi() {
        return api;
    }

    /**
     * Get the prefix used when registering a snapshot with an external API. This will also be
     * used when Bagging a snapshot in order to determine where the snapshot will be written to.
     *
     * @return the prefix
     */
    public String getChronopolisPrefix() {
        return chronopolisPrefix;
    }

    /**
     * The name to use when checking for a manifest in a Duracloud snapshot
     *
     * @return the manifest name, e.g. manifest-sha256.txt
     */
    public String getManifest() {
        return manifest;
    }

    /**
     * The path to the restore directory for a Duracloud bridge
     *
     * @return the restore path
     */
    @SuppressWarnings("unused")
    public String getRestores() {
        return restores;
    }

    /**
     * The path to the snapshot directory for a Duracloud bridge
     *
     * @return the snapshot path
     */
    public String getSnapshots() {
        return snapshots;
    }

    /**
     * Definition of what external apis a Duracloud snapshot should be pushed to
     *
     * @return the {@link Push} enum for this context
     */
    public Push getPush() {
        return push;
    }

    public BridgeContext setPush(Push push) {
        this.push = push;
        return this;
    }
}
