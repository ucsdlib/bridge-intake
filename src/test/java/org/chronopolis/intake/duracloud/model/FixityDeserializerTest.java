package org.chronopolis.intake.duracloud.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.serializers.ZonedDateTimeDeserializer;
import org.chronopolis.rest.models.serializers.ZonedDateTimeSerializer;
import org.chronopolis.rest.models.storage.Fixity;
import org.chronopolis.rest.models.storage.StagingStorageModel;
import org.chronopolis.tokenize.ManifestEntry;
import org.chronopolis.tokenize.ManifestEntryDeserializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

public class FixityDeserializerTest {

    @Test
    public void testSerializer() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        module.addDeserializer(Fixity.class, new FixityDeserializer());
        module.addDeserializer(ManifestEntry.class, new ManifestEntryDeserializer());
        mapper.registerModule(module);

        // Base fixity
        Fixity fixity = new Fixity("test-algorithm", "test-value", ZonedDateTime.now());
        String json = mapper.writeValueAsString(fixity);
        Fixity readFixity = mapper.readValue(json, Fixity.class);

        Assert.assertNotNull(readFixity);
        Assert.assertEquals(fixity.getAlgorithm(), readFixity.getAlgorithm());
        Assert.assertEquals(fixity.getValue(), readFixity.getValue());

        // Storage + Fixity
        StagingStorageModel storage = new StagingStorageModel()
                .setSize(1L)
                .setRegion(1L)
                .setActive(true)
                .setTotalFiles(1L)
                .setPath("test-path")
                .addFixity(fixity);
        String storageJson = mapper.writeValueAsString(storage);
        StagingStorageModel readStorage = mapper.readValue(storageJson, StagingStorageModel.class);

        Assert.assertNotNull(readStorage);

        // Bag + Storage + Fixity
        String bagJson = "{\n" +
                "  \"id\" : 17097,\n" +
                "  \"size\" : 17714674138,\n" +
                "  \"totalFiles\" : 85,\n" +
                "  \"bagStorage\" : {\n" +
                "    \"active\" : true,\n" +
                "    \"size\" : 17714674138,\n" +
                "    \"region\" : 1,\n" +
                "    \"totalFiles\" : 85,\n" +
                "    \"path\" : \"tdl-sfa-dcloud/sfa-dcloud_22_aug8.2018-oh1.to.oh69_2018-08-08-20-08-20\",\n" +
                "    \"fixities\" : [ {\n" +
                "      \"value\" : \"1a32a4aea4183129725087980d0574b336d8e308fbf29c94e21b473a8ba2d541\",\n" +
                "      \"algorithm\" : \"sha256\",\n" +
                "      \"createdAt\" : \"2018-08-09T08:45:01.81Z\"\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"createdAt\" : \"2018-08-09T15:45:01.543Z\",\n" +
                "  \"updatedAt\" : \"2018-08-09T15:45:01.543Z\",\n" +
                "  \"name\" : \"sfa-dcloud_22_aug8.2018-oh1.to.oh69_2018-08-08-20-08-20\",\n" +
                "  \"creator\" : \"tdl-bridge\",\n" +
                "  \"depositor\" : \"tdl-sfa-dcloud\",\n" +
                "  \"status\" : \"DEPOSITED\",\n" +
                "  \"requiredReplications\" : 3,\n" +
                "  \"replicatingNodes\" : [ ]\n" +
                "}";

        Bag bag = mapper.readValue(bagJson, Bag.class);
        Assert.assertNotNull(bag);
        Assert.assertNotNull(bag.getBagStorage());
        Assert.assertNotNull(bag.getBagStorage().getFixities());

        // And finally the ManifestEntry + Bag + Storage + Fixity
        ManifestEntry entry = new ManifestEntry(bag, "whatever-path", "whatever-digest");
        String entryJson = mapper.writeValueAsString(entry);
        ManifestEntry readEntry = mapper.readValue(entryJson, ManifestEntry.class);

        Assert.assertNotNull(readEntry);
    }

}