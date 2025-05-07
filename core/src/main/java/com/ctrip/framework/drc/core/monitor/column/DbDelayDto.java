package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.xpipe.api.codec.Codec;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DB-> DB drc delay dto
 */
public class DbDelayDto {
    private final long id;
    private final DelayInfo delayInfo;
    private final Long datachangeLasttime;


    public DbDelayDto(long id, DelayInfo delayInfo, Long datachangeLasttime) {
        this.id = id;
        this.delayInfo = delayInfo;
        this.datachangeLasttime = datachangeLasttime;
    }

    public static DbDelayDto from(long id, DelayInfo delayInfo, Long datachangeLasttime) {
        return new DbDelayDto(id, delayInfo, datachangeLasttime);
    }


    public String getDbName() {
        return delayInfo.getB();
    }

    public long getId() {
        return id;
    }

    public String getMha() {
        return delayInfo.getM();
    }

    public String getRegion() {
        return delayInfo.getR();
    }

    public String getDcName() {
        return delayInfo.getD();
    }

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    @Override
    public String toString() {
        return "DbDelayDto{" +
                "id=" + id +
                ", delayInfo=" + delayInfo +
                ", datachangeLasttime=" + datachangeLasttime +
                '}';
    }

    public static class DelayInfo {
        private static final Map<String, DelayInfo> FLYWEIGHT_CACHE = new ConcurrentHashMap<>();
        private static final Map<DelayInfo, String> FLYWEIGHT_CACHE_INFO_TO_JSON = new ConcurrentHashMap<>();
        // dc
        private String d;
        // region
        private String r;
        // mhaName
        private String m;
        // dbName
        private String b;

        public static DelayInfo parse(String json) {
            return FLYWEIGHT_CACHE.computeIfAbsent(json, newJson -> Codec.DEFAULT.decode(newJson, DelayInfo.class));
        }

        public static DelayInfo from(String dc, String region, String mhaName, String dbName) {
            DelayInfo delayInfo = new DelayInfo();
            delayInfo.setD(dc);
            delayInfo.setR(region);
            delayInfo.setM(mhaName);
            delayInfo.setB(dbName);
            return delayInfo;
        }

        public String toJson() {
            return FLYWEIGHT_CACHE_INFO_TO_JSON.computeIfAbsent(this, Codec.DEFAULT::encode);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DelayInfo)) return false;
            DelayInfo delayInfo = (DelayInfo) o;
            return Objects.equals(d, delayInfo.d) && Objects.equals(r, delayInfo.r) && Objects.equals(m, delayInfo.m) && Objects.equals(b, delayInfo.b);
        }

        @Override
        public String toString() {
            return "DelayInfo{" +
                    "d='" + d + '\'' +
                    ", r='" + r + '\'' +
                    ", m='" + m + '\'' +
                    ", b='" + b + '\'' +
                    '}';
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
