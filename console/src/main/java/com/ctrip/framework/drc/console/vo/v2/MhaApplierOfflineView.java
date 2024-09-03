package com.ctrip.framework.drc.console.vo.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.utils.DisposableFeature;

import java.util.List;

/**
 * @author yongnian
 * @create 2024/8/29 16:50
 */
@DisposableFeature
public class MhaApplierOfflineView {
    private List<ApplierTblV2> appliers;
    private int mhaApplierCount;
    private int mhaReplicationWithMhaApplierCount;
    private int mhaReplicationWithDbApplierCount;
    private int mhaReplicationWithBothMhaApplierAndDbApplierCount;
    private List<Long> mhaReplicationWithBothMhaApplierAndDbApplierIds;

    public List<Long> getMhaReplicationWithBothMhaApplierAndDbApplierIds() {
        return mhaReplicationWithBothMhaApplierAndDbApplierIds;
    }

    public void setMhaReplicationWithBothMhaApplierAndDbApplierIds(List<Long> mhaReplicationWithBothMhaApplierAndDbApplierIds) {
        this.mhaReplicationWithBothMhaApplierAndDbApplierIds = mhaReplicationWithBothMhaApplierAndDbApplierIds;
    }

    public int getMhaReplicationWithBothMhaApplierAndDbApplierCount() {
        return mhaReplicationWithBothMhaApplierAndDbApplierCount;
    }

    public void setMhaReplicationWithBothMhaApplierAndDbApplierCount(int mhaReplicationWithBothMhaApplierAndDbApplierCount) {
        this.mhaReplicationWithBothMhaApplierAndDbApplierCount = mhaReplicationWithBothMhaApplierAndDbApplierCount;
    }

    public int getMhaReplicationWithDbApplierCount() {
        return mhaReplicationWithDbApplierCount;
    }

    public void setMhaReplicationWithDbApplierCount(int mhaReplicationWithDbApplierCount) {
        this.mhaReplicationWithDbApplierCount = mhaReplicationWithDbApplierCount;
    }

    public void setMhaApplierCount(int mhaApplierCount) {
        this.mhaApplierCount = mhaApplierCount;
    }

    public List<ApplierTblV2> getAppliers() {
        return appliers;
    }

    public int getMhaApplierCount() {
        return mhaApplierCount;
    }

    public int getMhaReplicationWithMhaApplierCount() {
        return mhaReplicationWithMhaApplierCount;
    }

    public void setAppliers(List<ApplierTblV2> appliers) {
        this.appliers = appliers;
    }

    public void setMhaReplicationWithMhaApplierCount(int mhaReplicationWithMhaApplierCount) {
        this.mhaReplicationWithMhaApplierCount = mhaReplicationWithMhaApplierCount;
    }

    @Override
    public String toString() {
        return "MhaApplierOfflineView{" +
                "appliers=" + appliers +
                ", mhaApplierCount=" + mhaApplierCount +
                ", mhaReplicationWithMhaApplierCount=" + mhaReplicationWithMhaApplierCount +
                ", mhaReplicationWithDbApplierCount=" + mhaReplicationWithDbApplierCount +
                ", mhaReplicationWithBothMhaApplierAndDbApplierCount=" + mhaReplicationWithBothMhaApplierAndDbApplierCount +
                ", mhaReplicationWithBothMhaApplierAndDbApplierIds=" + mhaReplicationWithBothMhaApplierAndDbApplierIds +
                '}';
    }
}
