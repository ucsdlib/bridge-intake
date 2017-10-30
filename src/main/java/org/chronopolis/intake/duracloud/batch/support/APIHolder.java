package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.ServiceGenerator;

/**
 * Just to reduce some of the constructor params for the SnapshotJobManager
 * Because I'm insane
 *
 * Created by shake on 11/20/15.
 */
public class APIHolder {

    public final ServiceGenerator generator;
    public final BridgeAPI bridge;
    public final LocalAPI dpn;

    public APIHolder(ServiceGenerator generator, BridgeAPI bridge, LocalAPI dpn) {
        this.generator = generator;
        this.bridge = bridge;
        this.dpn = dpn;
    }

}
