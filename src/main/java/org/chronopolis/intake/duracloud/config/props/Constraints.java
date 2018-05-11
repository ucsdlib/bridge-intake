package org.chronopolis.intake.duracloud.config.props;

import org.chronopolis.bag.core.Unit;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for replication constraints at each node
 *
 * @author shake
 */
@ConfigurationProperties(prefix = "constraints")
public class Constraints {

    private List<Node> nodes = new ArrayList<>();

    public List<Node> getNodes() {
        return nodes;
    }

    public Constraints setNodes(List<Node> nodes) {
        this.nodes = nodes;
        return this;
    }

    public static class Node {
        private String name = "test-node";
        private SizeLimit sizeLimit = new SizeLimit();
        private List<String> members = new ArrayList<>();

        public String getName() {
            return name;
        }

        public Node setName(String name) {
            this.name = name;
            return this;
        }

        public SizeLimit getSizeLimit() {
            return sizeLimit;
        }

        public Node setSizeLimit(SizeLimit sizeLimit) {
            this.sizeLimit = sizeLimit;
            return this;
        }

        public List<String> getMembers() {
            return members;
        }

        public Node setMembers(List<String> members) {
            this.members = members;
            return this;
        }
    }

    public static class SizeLimit {
        private int size = -1;
        private Unit unit = Unit.BYTE;

        public int getSize() {
            return size;
        }

        public SizeLimit setSize(int size) {
            this.size = size;
            return this;
        }

        public Unit getUnit() {
            return unit;
        }

        public SizeLimit setUnit(Unit unit) {
            this.unit = unit;
            return this;
        }
    }

}
