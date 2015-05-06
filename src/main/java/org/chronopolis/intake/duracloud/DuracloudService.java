package org.chronopolis.intake.duracloud;

import retrofit.http.POST;

/**
 * Interface for interacting with the Duracloud Bridge Server
 *
 * Only the necessary REST calls
 *
 * Created by shake on 8/1/14.
 */
public interface DuracloudService {

    @POST("/api/snapshot/complete")
    void snapshotComplete();

    @POST("/api/restore/complete")
    void restoreComplete();

}
