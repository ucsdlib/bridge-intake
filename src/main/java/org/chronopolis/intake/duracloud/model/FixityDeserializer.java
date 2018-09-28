package org.chronopolis.intake.duracloud.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.chronopolis.rest.models.Fixity;
import org.chronopolis.rest.models.enums.FixityAlgorithm;

import java.io.IOException;
import java.time.ZonedDateTime;

/**
 * Not sure if this is necessary anymore
 *
 * @author shake
 */
public class FixityDeserializer extends JsonDeserializer<Fixity> {

    @Override
    public Fixity deserialize(JsonParser jsonParser,
                              DeserializationContext deserializationContext) throws IOException {
        ObjectCodec codec = jsonParser.getCodec();
        JsonNode treeNode = codec.readTree(jsonParser);

        JsonNode value = treeNode.get("value");
        JsonNode algNode = treeNode.get("algorithm");
        FixityAlgorithm algorithm = codec.readValue(algNode.traverse(codec), FixityAlgorithm.class);
        JsonNode createdAt = treeNode.get("createdAt");
        ZonedDateTime zdt = codec.readValue(createdAt.traverse(), ZonedDateTime.class);

        return new Fixity(value.asText(), algorithm, zdt);
    }
}
