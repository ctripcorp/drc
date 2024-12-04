package com.ctrip.framework.drc.core.server.config.applier.dto;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Created by shiruixin
 * 2024/11/20 16:53
 */
public class MessengerConfigDto extends FetcherConfigDto {
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
        if (!(o instanceof MessengerConfigDto)) return false;
        if (!super.equals(o)) return false;
        MessengerConfigDto that = (MessengerConfigDto) o;
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

    public boolean equalsIgnoreProperties(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessengerConfigDto)) return false;
        if (!super.equals(o)) return false;
        MessengerConfigDto that = (MessengerConfigDto) o;
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

    public boolean equalsProperties(Object o) {
        MessengerConfigDto that = (MessengerConfigDto) o;
        return Objects.equals(nameFilter, that.nameFilter) &&
                Objects.equals(properties, that.properties);
    }
}
