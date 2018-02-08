package org.chronopolis.intake.duracloud.config.props;

import org.chronopolis.bag.core.Unit;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Properties for configuration of a BagWriter
 *
 * @author shake
 */
@ConfigurationProperties(prefix = "bag")
public class BagProperties {

    /**
     * DPN Info properties
     */
    private Dpn dpn = new Dpn();

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

    public Dpn getDpn() {
        return dpn;
    }

    public BagProperties setDpn(Dpn dpn) {
        this.dpn = dpn;
        return this;
    }

    public static class Dpn {
        /**
         * The INGEST_NODE_NAME field
         */
        private String nodeName = "chronopolis";

        /**
         * The INGEST_NODE_ADDRESS field
         */
        private String nodeAddress;

        /**
         * The INGEST_NODE_CONTACT_NAME field
         */
        private String nodeContact;

        /**
         * The INGEST_NODE_CONTACT_EMAIL field; multiple allowed
         */
        private List<String> nodeEmail = new ArrayList<>();

        public String getNodeName() {
            return nodeName;
        }

        public Dpn setNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public String getNodeAddress() {
            return nodeAddress;
        }

        public Dpn setNodeAddress(String nodeAddress) {
            this.nodeAddress = nodeAddress;
            return this;
        }

        public String getNodeContact() {
            return nodeContact;
        }

        public Dpn setNodeContact(String nodeContact) {
            this.nodeContact = nodeContact;
            return this;
        }

        public List<String> getNodeEmail() {
            return nodeEmail;
        }

        public Dpn setNodeEmail(List<String> nodeEmail) {
            this.nodeEmail = nodeEmail;
            return this;
        }
    }
}
