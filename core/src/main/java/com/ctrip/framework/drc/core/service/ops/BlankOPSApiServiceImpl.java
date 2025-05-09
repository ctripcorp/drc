package com.ctrip.framework.drc.core.service.ops;

import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.statistics.traffic.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName BlankOPSApiServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 16:46
 * @Version: $
 */
public class BlankOPSApiServiceImpl implements OPSApiService {
    
    @Override
    public JsonNode getAllClusterInfo(String getAllClusterUrl, String accessToken) throws JsonProcessingException {
        return null;
    }

    @Override
    public JsonNode getAllDbs(String mysqlDbClusterUrl, String accessToken, String clusterName, String env) throws JsonProcessingException {
        return null;
    }

    @Override
    public List<AppNode> getAppNodes(String cmsGetServerUrl,String accessToken,List<String> appIds,String env) {
        return null;
    }

    @Override
    public List<HickWallTrafficEntity> getTrafficFromHickWall(HickWallTrafficContext context) throws Exception {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public Map<String, HickWallMessengerDelayEntity> getMessengerDelayFromHickWall(String getAllClusterUrl, String accessToken, List<String> mha, MqType mqType) throws IOException {
        return Map.of();
    }

    @Override
    public List<HickWallMhaReplicationDelayEntity> getMhaReplicationDelay(String getAllClusterUrl, String accessToken)
            throws IOException {
        return null;
    }

    @Override
    public List<HickWallConflictCount> getConflictCount(String apiUrl, String accessToken, boolean isTrx,
            boolean isCommit, int minutes) throws IOException {
        return null;
    }
    
}
