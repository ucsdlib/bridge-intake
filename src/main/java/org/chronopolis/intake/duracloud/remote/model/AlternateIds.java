package org.chronopolis.intake.duracloud.remote.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of the alternate ids a snapshot can have
 * Used when calling complete on a snapshot
 *
 * Created by shake on 7/27/15.
 */
public class AlternateIds {
    private final Logger log = LoggerFactory.getLogger(AlternateIds.class);

    List<String> alternateIds;

    public AlternateIds() {
        this.alternateIds = new ArrayList<>();
    }

    public AlternateIds(List<String> alternateIds) {
        this.alternateIds = alternateIds;
    }

    public List<String> getAlternateIds() {
        return alternateIds;
    }

    public AlternateIds setAlternateIds(final List<String> alternateIds) {
        this.alternateIds = alternateIds;
        return this;
    }

    public AlternateIds addAlternateId(String alternateId) {
        this.alternateIds.add(alternateId);
        return this;
    }
}
