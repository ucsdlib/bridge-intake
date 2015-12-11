package org.chronopolis.intake.duracloud.model;

/**
 * Class to encapsulate information about bagging
 *
 * Created by shake on 11/12/15.
 */
public class BagReceipt {

    private String name;
    private String receipt;

    public String getName() {
        return name;
    }

    public BagReceipt setName(String name) {
        this.name = name;
        return this;
    }

    public String getReceipt() {
        return receipt;
    }

    public BagReceipt setReceipt(String receipt) {
        this.receipt = receipt;
        return this;
    }

    public boolean isInitialized() {
        return name != null && receipt != null;
    }
}
