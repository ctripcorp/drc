package com.ctrip.framework.drc.console.vo.log;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/10/10 17:17
 */
public class ConflictCurrentRecordView {
    private List<Map<String, Object>> srcRecords;
    private List<Map<String, Object>> dstRecords;
    private boolean recordIsEqual;

    public List<Map<String, Object>> getSrcRecords() {
        return srcRecords;
    }

    public void setSrcRecords(List<Map<String, Object>> srcRecords) {
        this.srcRecords = srcRecords;
    }

    public List<Map<String, Object>> getDstRecords() {
        return dstRecords;
    }

    public void setDstRecords(List<Map<String, Object>> dstRecords) {
        this.dstRecords = dstRecords;
    }

    public boolean isRecordIsEqual() {
        return recordIsEqual;
    }

    public void setRecordIsEqual(boolean recordIsEqual) {
        this.recordIsEqual = recordIsEqual;
    }
}
