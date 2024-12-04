package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.meta.ApplierMeta;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class ApplierConfigDto extends FetcherConfigDto {

    @Override
    public String toString() {
        return "ApplierConfigDto{" +
                "gaqSize=" + gaqSize +
                ", workerCount=" + workerCount +
                ", workerSize=" + workerSize +
                ", gtidExecuted='" + gtidExecuted + '\'' +
                ", includedDbs='" + includedDbs + '\'' +
                ", nameFilter='" + nameFilter + '\'' +
                ", nameMapping='" + nameMapping + '\'' +
                ", routeInfo='" + routeInfo + '\'' +
                ", managerIp='" + managerIp + '\'' +
                ", managerPort=" + managerPort +
                ", mhaName=" + mhaName +
                ", replicator=" + replicator +
                ", target=" + target +
                ", name='" + name + '\'' +
                ", port=" + port +
                ", ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", cluster='" + cluster + '\'' +
                ", applyMode='" + applyMode + '\'' +
                ", properties='" + properties + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplierConfigDto)) return false;
        if (!super.equals(o)) return false;
        ApplierConfigDto that = (ApplierConfigDto) o;
        if (StringUtils.isBlank(gtidExecuted) && StringUtils.isNotBlank(that.gtidExecuted)) return false;
        return gaqSize == that.gaqSize &&
                workerCount == that.workerCount &&
                workerSize == that.workerSize &&
                Objects.equals(replicator.ip, that.replicator.ip) &&
                replicator.port == that.replicator.port &&
                Objects.equals(target.ip, that.target.ip) &&
                Objects.equals(includedDbs, that.includedDbs) &&
                Objects.equals(nameFilter, that.nameFilter) &&
                Objects.equals(nameMapping, that.nameMapping) &&
                Objects.equals(routeInfo, that.routeInfo) &&
                target.port == that.target.port &&
                applyMode == that.applyMode &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public boolean equalsIgnoreProperties(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplierConfigDto)) return false;
        if (!super.equals(o)) return false;
        ApplierConfigDto that = (ApplierConfigDto) o;
        if (StringUtils.isBlank(gtidExecuted) && StringUtils.isNotBlank(that.gtidExecuted)) return false;
        return gaqSize == that.gaqSize &&
                workerCount == that.workerCount &&
                workerSize == that.workerSize &&
                Objects.equals(replicator.ip, that.replicator.ip) &&
                replicator.port == that.replicator.port &&
                Objects.equals(target.ip, that.target.ip) &&
                Objects.equals(includedDbs, that.includedDbs) &&
                Objects.equals(nameMapping, that.nameMapping) &&
                Objects.equals(routeInfo, that.routeInfo) &&
                target.port == that.target.port &&
                applyMode == that.applyMode;
    }

    @Override
    public boolean equalsProperties(Object o) {
        ApplierConfigDto that = (ApplierConfigDto) o;
        return Objects.equals(nameFilter, that.nameFilter) &&
                Objects.equals(properties, that.properties);
    }

}
