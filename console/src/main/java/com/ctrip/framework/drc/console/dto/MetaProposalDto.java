package com.ctrip.framework.drc.console.dto;

import java.util.List;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-09
 */
public class MetaProposalDto {

    private String srcMha;

    private String destMha;

    private List<String> srcReplicatorIps;

    private List<String> srcApplierIps;

    private String srcApplierIncludedDbs;

    private String srcApplierNameFilter;

    private String srcApplierNameMapping;

    private int srcApplierApplyMode;

    private List<String> destReplicatorIps;

    private List<String> destApplierIps;

    private String destApplierIncludedDbs;

    private String destApplierNameFilter;

    private String destApplierNameMapping;

    private int destApplierApplyMode;

    private String srcGtidExecuted;

    private String destGtidExecuted;

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public List<String> getSrcReplicatorIps() {
        return srcReplicatorIps;
    }

    public void setSrcReplicatorIps(List<String> srcReplicatorIps) {
        this.srcReplicatorIps = srcReplicatorIps;
    }

    public List<String> getSrcApplierIps() {
        return srcApplierIps;
    }

    public void setSrcApplierIps(List<String> srcApplierIps) {
        this.srcApplierIps = srcApplierIps;
    }

    public List<String> getDestReplicatorIps() {
        return destReplicatorIps;
    }

    public void setDestReplicatorIps(List<String> destReplicatorIps) {
        this.destReplicatorIps = destReplicatorIps;
    }

    public List<String> getDestApplierIps() {
        return destApplierIps;
    }

    public void setDestApplierIps(List<String> destApplierIps) {
        this.destApplierIps = destApplierIps;
    }

    public String getSrcApplierIncludedDbs() {
        return srcApplierIncludedDbs;
    }

    public void setSrcApplierIncludedDbs(String srcApplierIncludedDbs) {
        this.srcApplierIncludedDbs = srcApplierIncludedDbs;
    }

    public String getDestApplierIncludedDbs() {
        return destApplierIncludedDbs;
    }

    public void setDestApplierIncludedDbs(String destApplierIncludedDbs) {
        this.destApplierIncludedDbs = destApplierIncludedDbs;
    }

    public int getSrcApplierApplyMode() {
        return srcApplierApplyMode;
    }

    public void setSrcApplierApplyMode(int srcApplierApplyMode) {
        this.srcApplierApplyMode = srcApplierApplyMode;
    }

    public int getDestApplierApplyMode() {
        return destApplierApplyMode;
    }

    public void setDestApplierApplyMode(int destApplierApplyMode) {
        this.destApplierApplyMode = destApplierApplyMode;
    }

    public String getSrcGtidExecuted() {
        return srcGtidExecuted;
    }

    public void setSrcGtidExecuted(String srcGtidExecuted) {
        this.srcGtidExecuted = srcGtidExecuted;
    }

    public String getDestGtidExecuted() {
        return destGtidExecuted;
    }

    public void setDestGtidExecuted(String destGtidExecuted) {
        this.destGtidExecuted = destGtidExecuted;
    }

    public String getSrcApplierNameFilter() {
        return srcApplierNameFilter;
    }

    public void setSrcApplierNameFilter(String srcApplierNameFilter) {
        this.srcApplierNameFilter = srcApplierNameFilter;
    }

    public String getDestApplierNameFilter() {
        return destApplierNameFilter;
    }

    public void setDestApplierNameFilter(String destApplierNameFilter) {
        this.destApplierNameFilter = destApplierNameFilter;
    }

    public String getSrcApplierNameMapping() {
        return srcApplierNameMapping;
    }

    public void setSrcApplierNameMapping(String srcApplierNameMapping) {
        this.srcApplierNameMapping = srcApplierNameMapping;
    }

    public String getDestApplierNameMapping() {
        return destApplierNameMapping;
    }

    public void setDestApplierNameMapping(String destApplierNameMapping) {
        this.destApplierNameMapping = destApplierNameMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaProposalDto that = (MetaProposalDto) o;
        return Objects.equals(srcMha, that.srcMha) &&
                Objects.equals(destMha, that.destMha) &&
                Objects.equals(srcReplicatorIps, that.srcReplicatorIps) &&
                Objects.equals(srcApplierIps, that.srcApplierIps) &&
                Objects.equals(srcApplierIncludedDbs, that.srcApplierIncludedDbs) &&
                Objects.equals(srcApplierNameFilter, that.srcApplierNameFilter) &&
                srcApplierApplyMode == that.srcApplierApplyMode &&
                Objects.equals(destReplicatorIps, that.destReplicatorIps) &&
                Objects.equals(destApplierIps, that.destApplierIps) &&
                Objects.equals(destApplierIncludedDbs, that.destApplierIncludedDbs) &&
                Objects.equals(destApplierNameFilter, that.destApplierNameFilter) &&
                Objects.equals(destApplierNameMapping, that.destApplierNameMapping) &&
                destApplierApplyMode == that.destApplierApplyMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcMha, destMha, srcReplicatorIps, srcApplierIps, srcApplierIncludedDbs, srcApplierNameFilter, srcApplierNameMapping, srcApplierApplyMode, destReplicatorIps, destApplierIps, destApplierIncludedDbs, destApplierNameFilter, destApplierNameMapping, destApplierApplyMode);
    }

    @Override
    public String toString() {
        return "MetaProposalDto{" +
                "srcMha='" + srcMha + '\'' +
                ", destMha='" + destMha + '\'' +
                ", srcReplicatorIps=" + srcReplicatorIps +
                ", srcApplierIps=" + srcApplierIps +
                ", srcApplierIncludedDbs='" + srcApplierIncludedDbs + '\'' +
                ", srcApplierNameFilter='" + srcApplierNameFilter + '\'' +
                ", srcApplierNameMapping='" + srcApplierNameMapping + '\'' +
                ", srcApplierApplyMode='" + srcApplierApplyMode + '\'' +
                ", destReplicatorIps=" + destReplicatorIps +
                ", destApplierIps=" + destApplierIps +
                ", destApplierIncludedDbs='" + destApplierIncludedDbs + '\'' +
                ", destApplierNameFilter='" + destApplierNameFilter + '\'' +
                ", destApplierNameMapping='" + destApplierNameMapping + '\'' +
                ", srcApplierApplyMode='" + srcApplierApplyMode + '\'' +
                ", destApplierApplyMode='" + destApplierApplyMode + '\'' +
                ", destGtidExecuted='" + destGtidExecuted + '\'' +
                '}';
    }
}
