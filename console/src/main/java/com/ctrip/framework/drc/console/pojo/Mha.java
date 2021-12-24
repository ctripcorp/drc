package com.ctrip.framework.drc.console.pojo;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-13
 */
public class Mha {

    private String mhaName;

    private String zoneId;

    private DbEndpoint master;

    private List<DbEndpoint> slaves;

    public Mha() {
    }

    public Mha(String mhaName, String zoneId, DbEndpoint master, List<DbEndpoint> slaves) {
        this.mhaName = mhaName;
        this.zoneId = zoneId;
        this.master = master;
        this.slaves = slaves;
    }

    public String getMhaName() {
        return mhaName;
    }

    public String getZoneId() {
        return zoneId;
    }

    public DbEndpoint getMaster() {
        return master;
    }

    public List<DbEndpoint> getSlaves() {
        return slaves;
    }

    @Override
    public String toString() {
        return "Mha{" +
                "mhaName='" + mhaName + '\'' +
                ", zoneId='" + zoneId + '\'' +
                ", master=" + master +
                ", slaves=" + slaves +
                '}';
    }

    public static class DbEndpoint {

        private String ip;

        private int port;

        private String readWeight;

        public DbEndpoint() {
        }

        public DbEndpoint(String ip, int port, String readWeight) {
            this.ip = ip;
            this.port = port;
            this.readWeight = readWeight;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public String getReadWeight() {
            return readWeight;
        }

        @Override
        public String toString() {
            return "{ip='" + ip + '\'' +
                    ", port=" + port +
                    ", readWeight='" + readWeight + '\'' +
                    '}';
        }
    }
}
