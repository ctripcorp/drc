package com.ctrip.framework.drc.core.service.ops;

import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiRequest;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;

/**
 * Created by dengquanliang
 * 2023/11/8 17:38
 */
public class BlankApprovalApiServiceImpl implements ApprovalApiService {

    @Override
    public ApprovalApiResponse createApproval(ApprovalApiRequest request) {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
