package org.chronopolis.intake.duracloud.remote;

import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.Snapshot;
import org.chronopolis.intake.duracloud.remote.model.SnapshotContent;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Path;
import retrofit.http.Query;

import java.util.List;

/**
 * API definition for the Brdige server
 *
 * Created by shake on 7/20/15.
 */
public interface BridgeAPI {

    @GET("/snapshot")
    List<Snapshot> getSnapshots(@Query("host") String host);

    @GET("/snapshot/{snapshotId}")
    SnapshotDetails getSnapshotDetails(@Path("snapshotId") String snapshotId);

    @GET("/snapshot/{snapshotId}/content")
    SnapshotContent getSnapshotContent(@Path("snapshotId") String snapshotId);

    @POST("/snapshot/{snapshotId}/complete")
    String completeSnapshot(@Path("snapshotId") String snapshotId, @Body AlternateIds alternateIds);

}
