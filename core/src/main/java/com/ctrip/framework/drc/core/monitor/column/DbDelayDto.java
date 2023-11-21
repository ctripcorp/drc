package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.xpipe.api.codec.Codec;

import java.util.Objects;

/**
 * DB-> DB drc delay dto
 */
public class DbDelayDto {
    private final long id;
    private final String mha;
    private final String dbName;
    private final String region;
    private final String dcName;
    private final Long datachangeLasttime;


    public DbDelayDto(long id, String mha, String dbName, String region, String dcName, Long datachangeLasttime) {
        this.id = id;
        this.mha = mha;
        this.dbName = dbName;
        this.region = region;
        this.dcName = dcName;
        this.datachangeLasttime = datachangeLasttime;
    }

    public static DbDelayDto from(long id, DelayInfo delayInfo, Long datachangeLasttime) {
        return new DbDelayDto(id, delayInfo.getM(), delayInfo.getB(), delayInfo.getR(), delayInfo.getD(), datachangeLasttime);
    }


    public String getDbName() {
        return dbName;
    }

    public long getId() {
        return id;
    }

    public String getMha() {
        return mha;
    }

    public String getRegion() {
        return region;
    }

    public String getDcName() {
        return dcName;
    }

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    @Override
    public String toString() {
        return "DbDelayDto{" +
                "id=" + id +
                ", mha='" + mha + '\'' +
                ", dbName='" + dbName + '\'' +
                ", region='" + region + '\'' +
                ", dcName='" + dcName + '\'' +
                ", datachangeLasttime=" + datachangeLasttime +
                '}';
    }

    public static class DelayInfo {
        // dc
        private String d;
        // region
        private String r;
        // mhaName
        private String m;
        // dbName
        private String b;

        public static DelayInfo parse(String json) {
            return Codec.DEFAULT.decode(json, DelayInfo.class);
        }
        public static DelayInfo from(String dc, String region, String mhaName, String dbName) {
            DelayInfo delayInfo = new DelayInfo();
            delayInfo.setD(dc);
            delayInfo.setR(region);
            delayInfo.setM(mhaName);
            delayInfo.setB(dbName);
            return delayInfo;
        }
        public String toJson(){
            return Codec.DEFAULT.encode(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DelayInfo)) return false;
            DelayInfo delayInfo = (DelayInfo) o;
            return Objects.equals(d, delayInfo.d) && Objects.equals(r, delayInfo.r) && Objects.equals(m, delayInfo.m) && Objects.equals(b, delayInfo.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(d, r, m, b);
        }

        public String getD() {
            return d;
        }

        public void setD(String d) {
            this.d = d;
        }

        public String getR() {
            return r;
        }

        public void setR(String r) {
            this.r = r;
        }

        public String getM() {
            return m;
        }

        public void setM(String m) {
            this.m = m;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }
    }
}
