package com.ctrip.framework.drc.console.dto;

import java.util.List;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
public class FailoverDto {

    private String clusterName;

    private List<String> failoverGroups;

    private List<String> recoverGroups;

    private List<NodeStatusInfo> groups;

    private List<Map<String, Map<String, String>>> checkResults;

    private Boolean isForced;

    private Map<String, String> extra;

    public FailoverDto() {
    }

    public FailoverDto(String clusterName, List<String> failoverGroups, List<String> recoverGroups, List<NodeStatusInfo> groups, List<Map<String, Map<String, String>>> checkResults, Boolean isForced, Map<String, String> extra) {
        this.clusterName = clusterName;
        this.failoverGroups = failoverGroups;
        this.recoverGroups = recoverGroups;
        this.groups = groups;
        this.checkResults = checkResults;
        this.isForced = isForced;
        this.extra = extra;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getFailoverGroups() {
        return failoverGroups;
    }

    public void setFailoverGroups(List<String> failoverGroups) {
        this.failoverGroups = failoverGroups;
    }

    public List<NodeStatusInfo> getGroups() {
        return groups;
    }

    public void setGroups(List<NodeStatusInfo> groups) {
        this.groups = groups;
    }

    public Boolean getIsForced() {
        return isForced;
    }

    public void setIsForced(Boolean isForced) {
        this.isForced = isForced;
    }

    public List<String> getRecoverGroups() {
        return recoverGroups;
    }

    public void setRecoverGroups(List<String> recoverGroups) {
        this.recoverGroups = recoverGroups;
    }

    public List<Map<String, Map<String, String>>> getCheckResults() {
        return checkResults;
    }

    public void setCheckResults(List<Map<String, Map<String, String>>> checkResults) {
        this.checkResults = checkResults;
    }

    public Boolean getForced() {
        return isForced;
    }

    public void setForced(Boolean forced) {
        isForced = forced;
    }

    public Map<String, String> getExtra() {
        return extra;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    @Override
    public String toString() {
        return "FailoverDto{" +
                "clusterName='" + clusterName + '\'' +
                ", failoverGroups=" + failoverGroups +
                ", recoverGroups=" + recoverGroups +
                ", groups=" + groups +
                ", checkResults=" + checkResults +
                ", isForced=" + isForced +
                ", extra=" + extra +
                '}';
    }

    public static final class NodeStatusInfo {
        private String name;
        private Boolean down;
        private String idc;
        List<String> nodes;

        public NodeStatusInfo() {
        }

        public NodeStatusInfo(String name, Boolean down, String idc, List<String> nodes) {
            this.name = name;
            this.down = down;
            this.idc = idc;
            this.nodes = nodes;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getDown() {
            return down;
        }

        public void setDown(Boolean down) {
            this.down = down;
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes;
        }

        public String getIdc() {
            return idc;
        }

        public void setIdc(String idc) {
            this.idc = idc;
        }

        @Override
        public String toString() {
            return "NodeStatusInfo{" +
                    "name='" + name + '\'' +
                    ", down=" + down +
                    ", idc='" + idc + '\'' +
                    ", nodes=" + nodes +
                    '}';
        }
    }
}
