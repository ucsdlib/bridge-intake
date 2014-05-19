package org.chronopolis.common.ace;


import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulate the needed fields for a JSON post and serialization using gson (or jersey??)
 *
 * Created by shake on 2/20/14.
 */
public class GsonCollection {
    private long id;
    private String digestAlgorithm;
    private String directory;
    private String name;
    private String group;
    private String storage;
    private Setting settings;

    public GsonCollection() {
        this.settings = new Setting();
    }

    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public void setDigestAlgorithm(final String digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(final String directory) {
        this.directory = directory;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(final String storage) {
        this.storage = storage;
    }

    public long getId() {
        return id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public Setting getSettings() {
        return settings;
    }

    public void setSettings(final Setting settings) {
        this.settings = settings;
    }

    public void addSetting(final String key, final String val) {
        Entry entry = new Entry(key, val);
        settings.entry.add(entry);
    }

    // We'll have a method for each of the entries we can add
    public void setAuditTokens(final String val) {
        Entry entry = new Entry("audit.tokens", val);
        settings.entry.add(entry);
    }

    public void setAuditPeriod(final String val) {
        Entry entry = new Entry("audit.period", val);
        settings.entry.add(entry);
    }

    public void setProxyData(final String val) {
        Entry entry = new Entry("proxy.data", val);
        settings.entry.add(entry);
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public class Setting {
        private final List<Entry> entry;

        public Setting() {
            entry = new ArrayList<>();
        }

    }

    public class Entry {
        private final String key;
        private final String value;

        public Entry(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
