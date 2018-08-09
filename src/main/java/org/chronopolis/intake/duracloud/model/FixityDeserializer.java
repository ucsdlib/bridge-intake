package org.chronopolis.intake.duracloud.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.chronopolis.rest.models.storage.Fixity;

import java.io.IOException;
import java.time.ZonedDateTime;

/**
 * @author shake
 */
public class FixityDeserializer extends JsonDeserializer<Fixity> {

    @Override
    public Fixity deserialize(JsonParser jsonParser,
                              DeserializationContext deserializationContext) throws IOException {
        ObjectCodec codec = jsonParser.getCodec();
        JsonNode treeNode = codec.readTree(jsonParser);

        JsonNode value = treeNode.get("value");
        JsonNode algorithm = treeNode.get("algorithm");
        JsonNode createdAt = treeNode.get("createdAt");
        ZonedDateTime zdt = codec.readValue(createdAt.traverse(), ZonedDateTime.class);

        return new Fixity(algorithm.asText(), value.asText(), zdt);
    }
}
