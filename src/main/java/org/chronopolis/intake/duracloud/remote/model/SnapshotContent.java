package org.chronopolis.intake.duracloud.remote.model;

import java.util.List;
import java.util.Map;

/**
 * Snapshot content from bridge/snapshot/{snapshotId}/content
 *
 * Created by shake on 7/20/15.
 */
public class SnapshotContent {

    private List<Detail> contentItems;

    public List<Detail> getContentItems() {
        return contentItems;
    }

    public SnapshotContent setContentItems(List<Detail> contentItems) {
        this.contentItems = contentItems;
        return this;
    }


    public class Detail {
        private String contentId;
        private Map<String, String> contentProperties;

        public String getContentId() {
            return contentId;
        }

        public Detail setContentId(String contentId) {
            this.contentId = contentId;
            return this;
        }

        public Map<String, String> getContentProperties() {
            return contentProperties;
        }

        public Detail setContentProperties(Map<String, String> contentProperties) {
            this.contentProperties = contentProperties;
            return this;
        }
    }


}
