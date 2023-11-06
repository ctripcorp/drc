package com.ctrip.framework.drc.console.api;

import com.ctrip.framework.drc.console.param.api.ApprovalOpenApiResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/6 15:38
 */
public class ApprovalOpenApiTest {

    @Test
    public void testCreate() {
        Map<String, Object> update = new HashMap<>();
        update.put("Submitter", "ql_deng");
        update.put("CC", "ql_deng@trip.com");
        update.put("Summary", "drc测试");
        update.put("Detail Description", "[详情链接](http://drc.fat-1.qa.nt.ctripcorp.com/#/v2/mhaReplications?preciseSearchMode=false)");
        update.put("TypeOfRequest", "冲突自动处理审批");
        update.put("SubType", "");
        update.put("Source", "DRC CONSOLE");
        update.put("Source_Url", "http://drc.fat-1.qa.nt.ctripcorp.com/#/v2/mhaReplications?preciseSearchMode=false");
        update.put("Approval Type", 2);
        update.put("Approver1", "ql_deng");

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("update", update);
        requestBody.put("action", "CTRIP:GeneralApprovalProcess");
        requestBody.put("username", "ql_deng");
        requestBody.put("fieldNames", Lists.newArrayList("Ticket_ID"));
        requestBody.put("order", "Request_ID");

        Map<String, Object> body = new HashMap<>();
        body.put("request_body", requestBody);
        body.put("access_token", "ccd863654c9de41eaadf5e60f62c8bcb");

        ApprovalOpenApiResponse res = HttpUtils.post("http://osg.ops.ctripcorp.com/api/11102", body, ApprovalOpenApiResponse.class);
        System.out.println(res);
    }
}
