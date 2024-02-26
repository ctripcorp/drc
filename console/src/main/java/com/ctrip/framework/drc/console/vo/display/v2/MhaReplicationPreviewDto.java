package com.ctrip.framework.drc.console.vo.display.v2;

import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.enums.DrcApplyModeEnum;
import com.ctrip.framework.drc.console.enums.DrcStatusEnum;

import java.util.List;

public class MhaReplicationPreviewDto {

    private String dbName;
    private String srcRegionName;
    private String dstRegionName;
    private List<MhaDto> srcOptionalMha;
    private List<MhaDto> dstOptionalMha;
    /**
     * @see com.ctrip.framework.drc.console.enums.DrcStatusEnum
     */
    private Integer drcStatus;

    private Integer drcApplyMode;

    // only when
    public Boolean normalCase() {
        return getSrcMha() != null && getDstMha() != null;
    }

    public MhaDto getSrcMha() {
        if (srcOptionalMha == null) {
            return null;
        }
        if (srcOptionalMha.size() != 1) {
            return null;
        }
        return srcOptionalMha.get(0);
    }

    public MhaDto getDstMha() {
        if (dstOptionalMha == null) {
            return null;
        }
        if (dstOptionalMha.size() != 1) {
            return null;
        }
        return dstOptionalMha.get(0);
    }

    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public String getDstRegionName() {
        return dstRegionName;
    }

    public void setDstRegionName(String dstRegionName) {
        this.dstRegionName = dstRegionName;
    }

    public List<MhaDto> getSrcOptionalMha() {
        return srcOptionalMha;
    }

    public void setSrcOptionalMha(List<MhaDto> srcOptionalMha) {
        this.srcOptionalMha = srcOptionalMha;
    }

    public List<MhaDto> getDstOptionalMha() {
        return dstOptionalMha;
    }

    public void setDstOptionalMha(List<MhaDto> dstOptionalMha) {
        this.dstOptionalMha = dstOptionalMha;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Integer getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Integer drcStatus) {
        this.drcStatus = drcStatus;
    }

    public void setDrcStatus(DrcStatusEnum drcStatusEnum) {
        this.drcStatus = drcStatusEnum.getCode();
    }

    public Integer getDrcApplyMode() {
        return drcApplyMode;
    }

    public void setDrcApplyMode(Integer drcApplyMode) {
        this.drcApplyMode = drcApplyMode;
    }

    public void setDrcApplyMode(DrcApplyModeEnum drcType) {
        this.drcApplyMode = drcType.getCode();
    }
    @Override
    public String toString() {
        return "MhaReplicationPreviewDto{" +
                "dbName='" + dbName + '\'' +
                ", srcRegionName='" + srcRegionName + '\'' +
                ", dstRegionName='" + dstRegionName + '\'' +
                ", srcOptionalMha=" + srcOptionalMha +
                ", dstOptionalMha=" + dstOptionalMha +
                ", drcStatus=" + drcStatus +
                '}';
    }
}
