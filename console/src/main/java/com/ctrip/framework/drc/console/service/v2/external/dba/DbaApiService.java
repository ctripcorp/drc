package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;

import java.util.List;

public interface DbaApiService {

    DbaClusterInfoResponse getClusterMembersInfo(String clusterName);

    List<ClusterInfoDto> getDatabaseClusterInfo(String dbName);

    List<DbClusterInfoDto> getDatabaseClusterInfoList(String dalClusterName);
    
    List<String> getDBsWithQueryPermission();

    boolean everUserTraffic(String region, String dbName, String tableName, long startTime, long endTime, boolean includeRead);
    
    // todo hdpan
    void changePassword(String mhaName,String userName ,String newPassword);
}
