package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;

/**
 * Created by jixinwang on 2021/2/23
 */
public class MhaGroupSqlOperator {
    private long mhaGroupId;
    private String mhaAName;
    private String mhaBName;
    private String mhaADc;
    private String mhaBDc;
    private WriteSqlOperatorWrapper mhaASqlOperatorWrapper;
    private WriteSqlOperatorWrapper mhaBSqlOperatorWrapper;

    public long getMhaGroupId() {
        return mhaGroupId;
    }

    public void setMhaGroupId(long mhaGroupId) {
        this.mhaGroupId = mhaGroupId;
    }

    public String getMhaAName() {
        return mhaAName;
    }

    public void setMhaAName(String mhaAName) {
        this.mhaAName = mhaAName;
    }

    public String getMhaBName() {
        return mhaBName;
    }

    public void setMhaBName(String mhaBName) {
        this.mhaBName = mhaBName;
    }

    public String getMhaADc() {
        return mhaADc;
    }

    public void setMhaADc(String mhaADc) {
        this.mhaADc = mhaADc;
    }

    public String getMhaBDc() {
        return mhaBDc;
    }

    public void setMhaBDc(String mhaBDc) {
        this.mhaBDc = mhaBDc;
    }

    public WriteSqlOperatorWrapper getMhaASqlOperatorWrapper() {
        return mhaASqlOperatorWrapper;
    }

    public void setMhaASqlOperatorWrapper(WriteSqlOperatorWrapper mhaASqlOperatorWrapper) {
        this.mhaASqlOperatorWrapper = mhaASqlOperatorWrapper;
    }

    public WriteSqlOperatorWrapper getMhaBSqlOperatorWrapper() {
        return mhaBSqlOperatorWrapper;
    }

    public void setMhaBSqlOperatorWrapper(WriteSqlOperatorWrapper mhaBSqlOperatorWrapper) {
        this.mhaBSqlOperatorWrapper = mhaBSqlOperatorWrapper;
    }
}
