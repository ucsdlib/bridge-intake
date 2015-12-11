package org.chronopolis.intake.duracloud.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * TODO: See if we can just do a generic History for this (maybe with type parameters)
 *
 * Created by shake on 11/20/15.
 */
public class ReplicationHistorySerializer implements JsonSerializer<ReplicationHistory> {
    @Override
    public JsonElement serialize(ReplicationHistory replicationHistory, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject obj = new JsonObject();

        JsonElement serial = jsonSerializationContext.serialize(replicationHistory.getHistory());
        obj.add("history", new JsonPrimitive(serial.toString()));
        obj.add("alternate", new JsonPrimitive(replicationHistory.getAlternate()));
        return obj;
    }
}
