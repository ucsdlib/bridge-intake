package org.chronopolis.intake.duracloud.config.props;

import org.chronopolis.bag.core.Unit;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for configuration of a BagWriter
 *
 * @author shake
 */
@ConfigurationProperties(prefix = "bag")
public class BagProperties {

    /**
     * The maximum size of a bag before partitioning
     */
    private Integer maxSize = 100;

    /**
     * The unit size
     */
    private Unit unit = Unit.TERABYTE;

    public Integer getMaxSize() {
        return maxSize;
    }

    public BagProperties setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public Unit getUnit() {
        return unit;
    }

    public BagProperties setUnit(Unit unit) {
        this.unit = unit;
        return this;
    }
}
