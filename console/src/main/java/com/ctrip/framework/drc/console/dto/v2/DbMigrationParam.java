package com.ctrip.framework.drc.console.dto.v2;

import java.util.List;

/**
 * @ClassName DbMigrationRequest
 * @Author haodongPan
 * @Date 2023/8/14 11:41
 * @Version: $
 */
public class DbMigrationParam {
    
    private List<String> dbs;
    private MigrateMhaInfo oldMha;
    private MigrateMhaInfo newMha;
    private String operator;

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }

    public MigrateMhaInfo getOldMha() {
        return oldMha;
    }

    public void setOldMha(MigrateMhaInfo oldMha) {
        this.oldMha = oldMha;
    }

    public MigrateMhaInfo getNewMha() {
        return newMha;
    }

    public void setNewMha(MigrateMhaInfo newMha) {
        this.newMha = newMha;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public static class MigrateMhaInfo {
        public String name;
        public String masterIp;
        public int masterPort;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMasterIp() {
            return masterIp;
        }

        public void setMasterIp(String masterIp) {
            this.masterIp = masterIp;
        }

        public int getMasterPort() {
            return masterPort;
        }

        public void setMasterPort(int masterPort) {
            this.masterPort = masterPort;
        }

        @Override
        public String toString() {
            return "MigrateMhaInfo{" +
                    "name='" + name + '\'' +
                    ", masterIp='" + masterIp + '\'' +
                    ", masterPort=" + masterPort +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "DbMigrationParam{" +
                "dbs=" + dbs +
                ", oldMha=" + oldMha +
                ", newMha=" + newMha +
                ", operator='" + operator + '\'' +
                '}';
    }
}
