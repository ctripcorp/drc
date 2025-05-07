package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CacheMetaService {

    Map<String, List<ReplicatorWrapper>> getAllReplicatorsInLocalRegion();

    Map<String, ReplicatorWrapper> getMasterReplicatorsToBeMonitored(List<String> mhaNamesToBeMonitored);

    Map<String, Set<String>> getDc2ReplicatorIps(List<String> clusterIds);

    Map<String, Set<String>> getMha2UuidsMap(Set<String> dcNames);

    Map<String, Map<String, Set<String>>> getMhaDbUuidsMap(Set<String> dcNames, Drc drc);

    MonitorMetaInfo getMonitorMetaInfo() throws SQLException;

    MonitorMetaInfo getDstMonitorMetaInfo() throws SQLException;

    Endpoint getMasterEndpoint(String mha);

    Endpoint getMasterEndpointForWrite(String mha);

    List<Endpoint> getMasterEndpointsInAllAccounts(String mha);
    
    boolean refreshMetaCache();

    Set<String> getSrcMhasHasReplication(String dstMha);
}
