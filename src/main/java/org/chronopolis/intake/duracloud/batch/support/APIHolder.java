package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.IngestAPI;

/**
 * Just to reduce some of the constructor params for the SnapshotJobManager
 * Because I'm insane
 *
 * Created by shake on 11/20/15.
 */
public class APIHolder {

    public final IngestAPI ingest;
    public final BridgeAPI bridge;
    public final LocalAPI dpn;

    public APIHolder(IngestAPI ingest, BridgeAPI bridge, LocalAPI dpn) {
        this.ingest = ingest;
        this.bridge = bridge;
        this.dpn = dpn;
    }

}
