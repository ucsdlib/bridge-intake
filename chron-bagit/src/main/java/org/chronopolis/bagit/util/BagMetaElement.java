package org.chronopolis.bagit.util;

/**
 * Simple class to create key: value pairs found in bagit elements
 *
 * @author shake
 */
public class BagMetaElement<T> {
    private String key;
    private T value;

    public BagMetaElement(String key, T value) {
        this.key = key;
        this.value = value;
    }

    public static BagMetaElement ParseBagMetaElement(String line) {
        if ( line == null || line.isEmpty() ) {
            throw new RuntimeException("Cannot parse empty line");
        }
        String[] elements = line.split(":");
        if ( elements.length < 0 || elements.length > 2) {
            throw new RuntimeException("Cannot parse line, invalid format"
                    + "\nLine should be "
                    + "\nkey: value");
        }

        String key = elements[0];
        String value = elements[1];
        BagMetaElement metadata = new BagMetaElement(key, value);
        
        return metadata;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the value
     */
    public T getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(key).append(": ")
                                                      .append(value);
        return builder.toString();
    }
    
}
