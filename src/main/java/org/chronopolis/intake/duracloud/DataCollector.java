package org.chronopolis.intake.duracloud;

import org.chronopolis.intake.duracloud.model.BagData;

/**
 * Interface to abstract away from how we get the {@link BagData}, this way
 * we can insert a class which implements this and assume it gives us back
 * the necessary info. This is done because requirements are not yet set in
 * stone, so the way we receive this data may be different down the line.
 *
 * Created by shake on 7/30/15.
 */
public interface DataCollector {

    BagData collectBagData(String snapshotId);

}
