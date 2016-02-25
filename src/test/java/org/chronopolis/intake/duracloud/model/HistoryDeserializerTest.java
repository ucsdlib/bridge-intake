package org.chronopolis.intake.duracloud.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.junit.Assert;
import org.junit.Test;

/**
 * moo
 *
 * Created by shake on 2/25/16.
 */
public class HistoryDeserializerTest {

    @Test
    public void testDeserialize() throws Exception {
        String json = "[{'snapshot-action':'SNAPSHOT_BAGGED'},{'snapshot-id':'test-snapshot-id'},{'bag-ids':['test-bag-1','test-bag-2']},{'manifest-checksums':['test-checksum-1', 'test-checksum-2']}]";

        Gson g = new GsonBuilder()
                .registerTypeAdapter(History.class, new HistoryDeserializer())
                .registerTypeAdapter(BaggingHistory.class, new BaggingHistoryDeserializer())
                .disableHtmlEscaping()
                .create();

        History history = g.fromJson(json, History.class);
        Assert.assertTrue(history instanceof BaggingHistory);

        BaggingHistory bagging = (BaggingHistory) history;
        Assert.assertEquals(2, bagging.getHistory().size());
        assertReceiptIsCorrect("test-bag-1", "test-checksum-1", bagging.getHistory().get(0));
        assertReceiptIsCorrect("test-bag-2", "test-checksum-2", bagging.getHistory().get(1));
    }

    private void assertReceiptIsCorrect(String name, String checksum, BagReceipt receipt) {
        Assert.assertEquals(name, receipt.getName());
        Assert.assertEquals(checksum, receipt.getReceipt());
    }

}