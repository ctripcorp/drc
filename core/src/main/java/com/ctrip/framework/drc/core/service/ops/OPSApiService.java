package com.ctrip.framework.drc.core.service.ops;

import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMhaReplicationDelayEntity;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMessengerDelayEntity;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallTrafficContext;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallTrafficEntity;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;

public interface OPSApiService extends Ordered {
    JsonNode getAllClusterInfo(String getAllClusterUrl,String accessToken) throws JsonProcessingException;

    JsonNode getAllDbs(String mysqlDbClusterUrl,String accessToken,String clusterName, String env) throws JsonProcessingException;

     List<AppNode> getAppNodes(String cmsGetServerUrl,String accessToken,List<String> appIds,String env);

    List<HickWallTrafficEntity> getTrafficFromHickWall(HickWallTrafficContext context) throws Exception;

    List<HickWallMessengerDelayEntity> getMessengerDelayFromHickWall(String getAllClusterUrl, String accessToken, List<String> mha) throws IOException;
    
    List<HickWallMhaReplicationDelayEntity> getMhaReplicationDelay(String getAllClusterUrl, String accessToken) throws IOException;
    
    List<HickWallConflictCount> getConflictCount(String apiUrl,String accessToken,boolean isTrx,boolean isCommit,int minutes) throws IOException;
    

}
