package org.chronopolis.intake.duracloud;

import retrofit2.Call;
import retrofit2.http.POST;

/**
 * Interface for interacting with the Duracloud Bridge Server
 *
 * Only the necessary REST calls
 *
 * Created by shake on 8/1/14.
 */
public interface DuracloudService {

    @POST("/api/snapshot/complete")
    Call<Void> snapshotComplete();

    @POST("/api/restore/complete")
    Call<Void> restoreComplete();

}
