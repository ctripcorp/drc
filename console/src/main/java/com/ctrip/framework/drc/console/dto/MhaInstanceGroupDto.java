package com.ctrip.framework.drc.console.dto;

import java.util.List;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-11-13
 */
public class MhaInstanceGroupDto {

    private String mhaName;

    private String zoneId;

    private String type;

    private MySQLInstance master;

    private List<MySQLInstance> slaves;

    public String getMhaName() {
        return mhaName;
    }

    public String getZoneId() {
        return zoneId;
    }

    public String getType() {
        return type;
    }

    public MySQLInstance getMaster() {
        return master;
    }

    public List<MySQLInstance> getSlaves() {
        return slaves;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMaster(MySQLInstance master) {
        this.master = master;
    }

    public void setSlaves(List<MySQLInstance> slaves) {
        this.slaves = slaves;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaInstanceGroupDto)) return false;
        MhaInstanceGroupDto that = (MhaInstanceGroupDto) o;
        return Objects.equals(getMhaName(), that.getMhaName()) &&
                Objects.equals(getZoneId(), that.getZoneId()) &&
                Objects.equals(getType(), that.getType()) &&
                Objects.equals(getMaster(), that.getMaster()) &&
                Objects.equals(getSlaves(), that.getSlaves());
    }

    @Override
    public String toString() {
        return "MhaInstanceGroupDto{" +
                "mhaName='" + mhaName + '\'' +
                ", zoneId='" + zoneId + '\'' +
                ", type='" + type + '\'' +
                ", master=" + master +
                ", slaves=" + slaves +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMhaName(), getZoneId(), getType(), getMaster(), getSlaves());
    }

    public static final class MySQLInstance {
        private String ip;
        private int port;
        private String idc;
        private String uuid;
        private String serverType;


        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public String getIdc() {
            return idc;
        }

        public MySQLInstance setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public MySQLInstance setPort(int port) {
            this.port = port;
            return this;
        }

        public MySQLInstance setIdc(String idc) {
            this.idc = idc;
            return this;
        }

        public String getUuid() {
            return uuid;
        }

        public MySQLInstance setUuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public String getServerType() {
            return serverType;
        }

        public void setServerType(String serverType) {
            this.serverType = serverType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MySQLInstance that = (MySQLInstance) o;
            return port == that.port && Objects.equals(ip, that.ip) && Objects.equals(idc, that.idc) && Objects.equals(uuid, that.uuid) &&
                    Objects.equals(serverType, that.serverType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, port, idc, uuid, serverType);
        }

        @Override
        public String toString() {
            return "MySQLInstance{" +
                    "ip='" + ip + '\'' +
                    ", port=" + port +
                    ", idc='" + idc + '\'' +
                    ", uuid='" + uuid + '\'' +
                    ", serverType='" + serverType + '\'' +
                    '}';
        }
    }
}

