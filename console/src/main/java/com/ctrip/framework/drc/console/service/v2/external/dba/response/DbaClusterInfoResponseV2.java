package com.ctrip.framework.drc.console.service.v2.external.dba.response;


import java.util.List;
import java.util.stream.Collectors;


public class DbaClusterInfoResponseV2 {
    private String message;
    private List<MemberInfoV2> data;
    private Boolean success;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<MemberInfoV2> getData() {
        return data;
    }

    public void setData(List<MemberInfoV2> data) {
        this.data = data;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public DbaClusterInfoResponse toV1() {
        DbaClusterInfoResponse response = new DbaClusterInfoResponse();
        response.setMessage(message);
        response.setSuccess(success);
        Data data1 = new Data();
        data1.setMemberlist(this.data.stream().map(MemberInfoV2::toV1).collect(Collectors.toList()));
        response.setData(data1);
        return response;
    }
}
