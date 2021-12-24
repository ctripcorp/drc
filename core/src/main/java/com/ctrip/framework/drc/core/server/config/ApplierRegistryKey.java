package com.ctrip.framework.drc.core.server.config;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DOT;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class ApplierRegistryKey extends RegistryKey {

    private String replicatorMhaName;  //replicator mha name

    private ApplierRegistryKey(String mhaName, String clusterName, String replicatorMhaName) {
        super(clusterName, mhaName);
        this.replicatorMhaName = replicatorMhaName;
    }

    public static String from(String mhaName, String clusterName, String replicatorMhaName) {
        return new ApplierRegistryKey(mhaName, clusterName, replicatorMhaName).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplierRegistryKey)) return false;
        if (!super.equals(o)) return false;
        ApplierRegistryKey that = (ApplierRegistryKey) o;
        return Objects.equals(replicatorMhaName, that.replicatorMhaName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), replicatorMhaName);
    }

    @Override
    public String toString() {
        return StringUtils.join(new String[]{super.toString(), replicatorMhaName}, DOT);
    }
}
