package org.chronopolis.intake.duracloud.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Created by shake on 2/25/16.
 */
public class ReplicationHistorySerializerTest {

    @Test
    public void testSerialize() throws Exception {
        // that line count though
        String history = "{\"history\":\"[{'snapshot-action':'SNAPSHOT_REPLICATED'},{'snapshot-id':'test-snapshot-id'},{'bag-ids':['test-bag-1','test-bag-2']},{'node':'test-node'}]\",\"alternate\":false}";

        Gson g = new GsonBuilder()
                .disableHtmlEscaping()
                .registerTypeAdapter(ReplicationHistory.class, new ReplicationHistorySerializer())
                .create();

        ReplicationHistory rh = new ReplicationHistory("test-snapshot-id", "test-node", false);
        rh.addReceipt("test-bag-1");
        rh.addReceipt("test-bag-2");

        String json = g.toJson(rh);

        Assert.assertEquals(history, json);
    }

}