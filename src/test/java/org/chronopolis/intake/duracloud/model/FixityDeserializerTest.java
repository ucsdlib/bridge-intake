package org.chronopolis.intake.duracloud.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.Fixity;
import org.chronopolis.rest.models.StagingStorage;
import org.chronopolis.rest.models.enums.FixityAlgorithm;
import org.chronopolis.rest.models.serializers.FixityAlgorithmDeserializer;
import org.chronopolis.rest.models.serializers.FixityAlgorithmSerializer;
import org.chronopolis.rest.models.serializers.ZonedDateTimeDeserializer;
import org.chronopolis.rest.models.serializers.ZonedDateTimeSerializer;
import org.chronopolis.tokenize.ManifestEntry;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

import static com.google.common.collect.ImmutableSet.of;

public class FixityDeserializerTest {

    @Test
    public void testSerializer() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(FixityAlgorithm.class, new FixityAlgorithmSerializer());
        module.addDeserializer(FixityAlgorithm.class, new FixityAlgorithmDeserializer());
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        module.addDeserializer(Fixity.class, new FixityDeserializer());
        module.addDeserializer(ManifestEntry.class, new ManifestEntryDeserializer());
        mapper.registerModule(new KotlinModule());
        mapper.registerModule(module);

        // Base fixity
        Fixity fixity = new Fixity(
                "1a32a4aea4183129725087980d0574b336d8e308fbf29c94e21b473a8ba2d541",
                FixityAlgorithm.SHA_256,
                ZonedDateTime.now());
        String json = mapper.writeValueAsString(fixity);
        Fixity readFixity = mapper.readValue(json, Fixity.class);

        Assert.assertNotNull(readFixity);
        Assert.assertEquals(fixity.getAlgorithm(), readFixity.getAlgorithm());
        Assert.assertEquals(fixity.getValue(), readFixity.getValue());

        // Storage + Fixity
        StagingStorage storage = new StagingStorage(true, 1L, 1L, 1L, "test-path", of(fixity));
        String storageJson = mapper.writeValueAsString(storage);
        StagingStorage readStorage = mapper.readValue(storageJson, StagingStorage.class);

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
                "      \"algorithm\" : \"SHA-256\",\n" +
                "      \"createdAt\" : \"2018-08-09T08:45:01.81Z\"\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"createdAt\" : \"2018-08-09T15:45:01.543Z\",\n" +
                "  \"updatedAt\" : \"2018-08-09T15:45:01.543Z\",\n" +
                "  \"name\" : \"sfa-dcloud_22_aug8.2018-oh1.to.oh69_2018-08-08-20-08-20\",\n" +
                "  \"creator\" : \"tdl-bridge\",\n" +
                "  \"depositor\" : \"tdl-sfa-dcloud\",\n" +
                "  \"status\" : \"DEPOSITED\",\n" +
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