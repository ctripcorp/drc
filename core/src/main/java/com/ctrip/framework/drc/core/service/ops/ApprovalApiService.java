package com.ctrip.framework.drc.core.service.ops;

import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiRequest;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;
import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * Created by dengquanliang
 * 2023/11/8 17:06
 */
public interface ApprovalApiService extends Ordered {
    ApprovalApiResponse createApproval(ApprovalApiRequest request);
}
