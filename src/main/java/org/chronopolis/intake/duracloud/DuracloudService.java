package org.chronopolis.intake.duracloud;

import retrofit.http.POST;

/**
 * Created by shake on 8/1/14.
 */
public interface DuracloudService {

    @POST("/api/snapshot/complete")
    void snapshotComplete();

    @POST("/api/restore/complete")
    void restoreComplete();

}
