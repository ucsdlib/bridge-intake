package org.chronopolis.intake.duracloud.remote;

import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.intake.duracloud.remote.model.SnapshotContent;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.intake.duracloud.remote.model.SnapshotHistory;
import org.chronopolis.intake.duracloud.remote.model.SnapshotStatus;
import org.chronopolis.intake.duracloud.remote.model.Snapshots;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;

import java.util.Map;

/**
 * API definition for the Bridge server
 *
 * Created by shake on 7/20/15.
 */
public interface BridgeAPI {

    @GET("snapshot")
    Call<Snapshots> getSnapshots(@Query("host") String host, @Query("status") SnapshotStatus status);

    @GET("snapshot/{snapshotId}")
    Call<SnapshotDetails> getSnapshotDetails(@Path("snapshotId") String snapshotId);

    @GET("snapshot/{snapshotId}/content")
    Call<SnapshotContent> getSnapshotContent(@Path("snapshotId") String snapshotId);

    @POST("snapshot/{snapshotId}/complete")
    Call<SnapshotComplete> completeSnapshot(@Path("snapshotId") String snapshotId, @Body AlternateIds alternateIds);

    @GET("snapshot/{snapshotId}/history")
    Call<SnapshotHistory> getSnapshotHistory(@Path("snapshotId") String snapshotId, @QueryMap Map<String, String> params);

    @POST("snapshot/{snapshotId}/history")
    Call<HistorySummary> postHistory(@Path("snapshotId") String snapshotId, @Body History history);

}
