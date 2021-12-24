package com.ctrip.framework.drc.core.service.beacon;

import java.util.List;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-10-19
 */
public class RegisterDto {
    private List<NodeGroup> nodeGroups;

    private Extra extra;

    public RegisterDto(List<NodeGroup> nodeGroups, Extra extra) {
        this.nodeGroups = nodeGroups;
        this.extra = extra;
    }

    public List<NodeGroup> getNodeGroups() {
        return nodeGroups;
    }

    public Extra getExtra() {
        return extra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RegisterDto)) return false;
        RegisterDto that = (RegisterDto) o;
        return Objects.equals(getNodeGroups(), that.getNodeGroups()) &&
                Objects.equals(getExtra(), that.getExtra());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeGroups(), getExtra());
    }

    @Override
    public String toString() {
        return "RegisterDto{" +
                "nodeGroups=" + nodeGroups +
                ", extra=" + extra +
                '}';
    }

    public static final class NodeGroup {
        private String name;
        private String idc;
        private List<String> nodes;

        public NodeGroup(String name, String idc, List<String> nodes) {
            this.name = name;
            this.idc = idc;
            this.nodes = nodes;
        }

        public String getName() {
            return name;
        }

        public String getIdc() {
            return idc;
        }

        public List<String> getNodes() {
            return nodes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NodeGroup)) return false;
            NodeGroup nodeGroup = (NodeGroup) o;
            return Objects.equals(getName(), nodeGroup.getName()) &&
                    Objects.equals(getIdc(), nodeGroup.getIdc()) &&
                    Objects.equals(getNodes(), nodeGroup.getNodes());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName(), getIdc(), getNodes());
        }

        @Override
        public String toString() {
            return "NodeGroup{" +
                    "name='" + name + '\'' +
                    ", idc='" + idc + '\'' +
                    ", nodes=" + nodes +
                    '}';
        }
    }

    public static final class Extra {
        private String username;
        private String password;
        private String type;

        public Extra() {
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Extra)) return false;
            Extra extra = (Extra) o;
            return Objects.equals(getUsername(), extra.getUsername()) &&
                    Objects.equals(getPassword(), extra.getPassword()) &&
                    Objects.equals(getType(), extra.getType());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getUsername(), getPassword(), getType());
        }

        @Override
        public String toString() {
            return "Extra{" +
                    "username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }
}
