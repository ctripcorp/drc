package com.ctrip.framework.drc.core.server.config;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DOT;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class RegistryKey {

    private String clusterName;

    private String mhaName;

    protected RegistryKey(String clusterName, String mhaName) {
        this.clusterName = clusterName;
        this.mhaName = mhaName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public static RegistryKey from(String path) {
        if (StringUtils.isNotBlank(path)) {
            String[] ids = path.split("\\" + DOT);
            if (ids.length >= 2) { //local idc : clusterName.mhaName[.replicatorMhaName]
                return new RegistryKey(ids[0], ids[1]);
            }
        }
        throw new IllegalArgumentException("Illegal path " + path);
    }

    public static String from(String clusterName, String mhaName) {
        return new RegistryKey(clusterName, mhaName).toString();
    }

    public static String getTargetMha(String path) {
        if (StringUtils.isNotBlank(path)) {
            String[] ids = path.split("\\" + DOT);
            if (ids.length > 2) { //local idc : clusterName.mhaName[.replicatorMhaName]
                return ids[2];
            }
        }
        throw new IllegalArgumentException("Illegal path " + path);
    }

    public static String getTargetDB(String path) {
        if (StringUtils.isNotBlank(path)) {
            String[] ids = path.split("\\" + DOT);
            if (ids.length > 3) { //local idc : clusterName.mhaName.replicatorMhaName[.db]
                return ids[3];
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegistryKey that = (RegistryKey) o;
        return Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(mhaName, that.mhaName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(clusterName, mhaName);
    }

    @Override
    public String toString() {
        return StringUtils.join(new String[]{clusterName, mhaName}, DOT);
    }
}
