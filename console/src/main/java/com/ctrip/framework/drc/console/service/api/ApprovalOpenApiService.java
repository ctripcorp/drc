package com.ctrip.framework.drc.console.service.api;

import com.ctrip.framework.drc.console.param.api.ApprovalOpenApiRequest;
import com.ctrip.framework.drc.console.param.api.ApprovalOpenApiResponse;

/**
 * Created by dengquanliang
 * 2023/11/6 15:24
 */
public interface ApprovalOpenApiService {

    ApprovalOpenApiResponse createApproval(ApprovalOpenApiRequest request);
}
