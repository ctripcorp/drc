package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;

public interface DbaApiService {
    
    DbaClusterInfoResponse getClusterMembersInfo(String clusterName);
    
}
