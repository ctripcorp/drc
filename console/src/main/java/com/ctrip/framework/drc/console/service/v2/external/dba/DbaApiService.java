package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
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

    MhaAccounts accountV2PwdChange(MhaTblV2 mhaTblV2) ;

    MhaAccounts accountV2PwdChange(String mhaName, String masterNodeIp,Integer masterNodePort);
    
}
