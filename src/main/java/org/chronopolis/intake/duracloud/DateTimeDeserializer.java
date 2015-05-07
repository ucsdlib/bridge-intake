package org.chronopolis.intake.duracloud;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.Type;

/**
 * Created by shake on 5/7/15.
 */
public class DateTimeDeserializer implements JsonDeserializer<DateTime> {
    DateTimeFormatter fmt = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC();

    @Override
    public DateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        return fmt.parseDateTime(jsonElement.getAsJsonPrimitive().getAsString());
    }

}
