package org.chronopolis.intake.duracloud.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * [{snapshot-action}, {snapshot-id}, {bag-ids}, {manifest-checksums}]
 *
 * Created by shake on 2/23/16.
 */
public class BaggingHistoryDeserializer implements JsonDeserializer<BaggingHistory> {
    private final Logger log = LoggerFactory.getLogger(BaggingHistoryDeserializer.class);

    @Override
    public BaggingHistory deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonArray asArray = jsonElement.getAsJsonArray();
        // initialize everything so teh ide doesn't complain
        String snapshotId = null;
        List<String> ids = new ArrayList<>();
        List<String> checksums = new ArrayList<>();

        for (JsonElement element : asArray) {
            JsonObject asObject = element.getAsJsonObject();
            // This should only have one entry
            for (Map.Entry<String, JsonElement> entry : asObject.entrySet()) {
                switch (entry.getKey()) {
                    case "snapshot-id": snapshotId = entry.getValue().getAsString();
                        break;
                    case "bag-ids":
                        ids = jsonDeserializationContext.deserialize(entry.getValue(), List.class);
                        break;
                    case "manifest-checksums":
                        checksums = jsonDeserializationContext.deserialize(entry.getValue(), List.class);
                        break;
                    default:
                        break;
                }
            }
        }

        // idk
        if (ids.size() != checksums.size()) {
            return null;
        }

        BaggingHistory history = new BaggingHistory(snapshotId, false);
        for (int i=0; i < ids.size(); i++) {
            String bagId = ids.get(i);
            String checksum = checksums.get(i);
            history.addBaggingData(bagId, checksum);
        }

        return history;
    }
}
