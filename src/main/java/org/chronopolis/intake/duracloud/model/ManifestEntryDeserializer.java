package org.chronopolis.intake.duracloud.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.tokenize.ManifestEntry;

import java.io.IOException;

/**
 * @author shake
 */
public class ManifestEntryDeserializer extends JsonDeserializer<ManifestEntry> {
    @Override
    public ManifestEntry deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode treeNode = codec.readTree(p);

        JsonNode bagNode = treeNode.get("bag");
        Bag bag = codec.readValue(bagNode.traverse(codec), Bag.class);
        JsonNode pathNode = treeNode.get("path");
        JsonNode registeredDigest = treeNode.get("digest");
        return new ManifestEntry(bag, pathNode.asText(), registeredDigest.asText());
    }
}
