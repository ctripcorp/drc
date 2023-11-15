package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/11/6 17:45
 */
public class ConflictApprovalCallBackRequest {
    private String approvalCtripId;
    private String approvalStatus;
    private String rejectReason;
    private String approvalDate;
    private Data data;

    public String getApprovalCtripId() {
        return approvalCtripId;
    }

    public void setApprovalCtripId(String approvalCtripId) {
        this.approvalCtripId = approvalCtripId;
    }

    public String getApprovalStatus() {
        return approvalStatus;
    }

    public void setApprovalStatus(String approvalStatus) {
        this.approvalStatus = approvalStatus;
    }

    public String getRejectReason() {
        return rejectReason;
    }

    public void setRejectReason(String rejectReason) {
        this.rejectReason = rejectReason;
    }

    public String getApprovalDate() {
        return approvalDate;
    }

    public void setApprovalDate(String approvalDate) {
        this.approvalDate = approvalDate;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public static class Data {
        private Long approvalId;

        public Long getApprovalId() {
            return approvalId;
        }

        public void setApprovalId(Long approvalId) {
            this.approvalId = approvalId;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "approvalId=" + approvalId +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "ConflictApprovalCallBackRequest{" +
                "approvalCtripId='" + approvalCtripId + '\'' +
                ", approvalStatus='" + approvalStatus + '\'' +
                ", rejectReason='" + rejectReason + '\'' +
                ", approvalDate='" + approvalDate + '\'' +
                ", data=" + data +
                '}';
    }
}
