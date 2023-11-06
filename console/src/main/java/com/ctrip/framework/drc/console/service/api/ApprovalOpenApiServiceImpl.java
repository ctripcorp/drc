package com.ctrip.framework.drc.console.service.api;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.api.ApprovalOpenApiRequest;
import com.ctrip.framework.drc.console.param.api.ApprovalOpenApiResponse;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/6 16:20
 */
@Service
public class ApprovalOpenApiServiceImpl implements ApprovalOpenApiService {

    private static final String ACTION = "CTRIP:GeneralApprovalProcess";
    private static final String SOURCE = "DRC CONSOLE";
    private static final int APPROVAL_TYPE = 2;
    private static final String TITLE = "DRC冲突自动处理审批";
    private static final String SUMMARY = "冲突处理SQL";
    private static final String TICKET_ID = "Ticket_ID";
    private static final String ORDER = "Request_ID";
    private static final String DETAIL = "[详情链接](%s)";

    private UserService userService = ApiContainer.getUserServiceImpl();

    @Autowired
    private DomainConfig domainConfig;

    @Override
    public ApprovalOpenApiResponse createApproval(ApprovalOpenApiRequest request) {
        String url = domainConfig.getOpsApprovalUrl();
        String sourceUrl = domainConfig.getConflictDetailUrl();
        String username = userService.getInfo();

        String approvers = Joiner.on(";").join(request.getApprovers());

        Map<String, Object> update = new HashMap<>();
        update.put("Submitter", username);
        update.put("CC", domainConfig.getConflictCcEmail());
        update.put("Summary", SUMMARY);
        update.put("Detail Description", String.format(DETAIL, sourceUrl));
        update.put("TypeOfRequest", TITLE);
        update.put("SubType", "");
        update.put("Source", SOURCE);
        update.put("Source_Url", sourceUrl);
        update.put("Approval Type", APPROVAL_TYPE);
        update.put("Approver1", approvers);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("update", update);
        requestBody.put("action", ACTION);
        requestBody.put("username", username);
        requestBody.put("fieldNames", Lists.newArrayList(TICKET_ID));
        requestBody.put("order", ORDER);

        Map<String, Object> body = new HashMap<>();
        body.put("request_body", requestBody);
        body.put("access_token", domainConfig.getOpsApprovalToken());

        return HttpUtils.post(url, body, ApprovalOpenApiResponse.class);
    }
}
