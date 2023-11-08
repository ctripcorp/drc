package com.ctrip.framework.drc.service.console.service;

import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.ops.ApprovalApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiRequest;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/8 17:05
 */
public class ApprovalApiServiceImpl implements ApprovalApiService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String ACTION = "CTRIP:GeneralApprovalProcess";
    private static final String SOURCE = "DRC CONSOLE";
    private static final int APPROVAL_TYPE = 2;
    private static final String TITLE = "DRC冲突自动处理审批";
    private static final String SUMMARY = "冲突处理SQL";
    private static final String TICKET_ID = "Ticket_ID";
    private static final String ORDER = "Request_ID";
    private static final String DETAIL = "[详情链接](%s)";

    @Override
    public ApprovalApiResponse createApproval(ApprovalApiRequest request) {
        logger.info("createApproval request: {}", request);

        Map<String, Object> update = new HashMap<>();
        update.put("Submitter", request.getUsername());
        update.put("CC", request.getCcEmail());
        update.put("Summary", SUMMARY);
        update.put("Detail Description", String.format(DETAIL, request.getSourceUrl()));
        update.put("TypeOfRequest", TITLE);
        update.put("SubType", "");
        update.put("Source", SOURCE);
        update.put("Source_Url", request.getSourceUrl());
        update.put("Approval Type", APPROVAL_TYPE);
        update.put("Approver1", request.getApprover1());
        update.put("Approver2", request.getApprover2());
        update.put("bizData", request.getData());
        update.put("callbackUrl", request.getCallBackUrl());

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("update", update);
        requestBody.put("action", ACTION);
        requestBody.put("username", request.getUsername());
        requestBody.put("fieldNames", Lists.newArrayList(TICKET_ID));
        requestBody.put("order", ORDER);

        Map<String, Object> body = new HashMap<>();
        body.put("request_body", requestBody);
        body.put("access_token", request.getToken());

        ApprovalApiResponse response = HttpUtils.post(request.getUrl(), body, ApprovalApiResponse.class);
        return response;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
