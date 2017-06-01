package org.chronopolis.intake.duracloud.batch.support;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

/**
 * Weight for our replication distribution
 *
 * Created by shake on 5/31/17.
 */
public class Weight {

    private final String node;
    private final String snapshot;
    private final HashCode code;

    public Weight(String node, String snapshot) {
        this.node = node;
        this.snapshot = snapshot;

        this.code = Hashing.sha256().hashString(snapshot + node, Charset.forName("UTF-8"));
    }

    public String getNode() {
        return node;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public HashCode getCode() {
        return code;
    }
}
