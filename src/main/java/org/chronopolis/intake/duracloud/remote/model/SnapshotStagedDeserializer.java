package org.chronopolis.intake.duracloud.remote.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 * Created by shake on 2/26/16.
 */
public class SnapshotStagedDeserializer implements JsonDeserializer<SnapshotStaged> {

    @Override
    public SnapshotStaged deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonArray asArray = jsonElement.getAsJsonArray();
        // initialize everything so teh ide doesn't complain
        String snapshotId = null;

        for (JsonElement element : asArray) {
            JsonObject asObject = element.getAsJsonObject();
            // This should only have one entry
            for (Map.Entry<String, JsonElement> entry : asObject.entrySet()) {
                switch (entry.getKey()) {
                    case "snapshot-id": snapshotId = entry.getValue().getAsString();
                        break;
                    default:
                        break;
                }
            }
        }

        SnapshotStaged snapshot = new SnapshotStaged();
        snapshot.setSnapshotId(snapshotId);

        return snapshot;
    }

}
