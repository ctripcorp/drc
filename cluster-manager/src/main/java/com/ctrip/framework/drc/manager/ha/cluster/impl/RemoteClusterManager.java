package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.cluster.META_SERVER_SERVICE;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerService;
import com.ctrip.framework.drc.manager.ha.rest.CircularForwardException;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.rest.ForwardType;
import com.google.common.collect.Lists;
import org.springframework.http.*;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class RemoteClusterManager extends AbstractRemoteClusterManager implements ClusterManager {

    private String changeClusterPath;
    private String upstreamChangePath;
    private String getActiveReplicatorPath;
    private String getActiveMySQLPath;


    public RemoteClusterManager(String currentServerId, String serverId) {
        super(currentServerId, serverId);
    }

    public RemoteClusterManager(String currentServerId, String serverId, ClusterServerInfo clusterServerInfo) {
        super(currentServerId, serverId, clusterServerInfo);

        if(getHttpHost() != null){
            changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(getHttpHost());
            upstreamChangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(getHttpHost());
            getActiveReplicatorPath = META_SERVER_SERVICE.GET_ACTIVE_REPLICATOR.getRealPath(getHttpHost());
            getActiveMySQLPath = META_SERVER_SERVICE.GET_ACTIVE_MYSQL.getRealPath(getHttpHost());
        }
    }

    private String appendDcIdAndOperator(String uriPath, String dcId, String operator) {
        return uriPath + "?dcId=" + dcId + "&operator=" + operator;
    }

    @Override
    public void clusterAdded(String dcId, String clusterId, ForwardInfo forwardInfo, String operator) {

        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
        logger.info("[clusterAdded][forward]{},{}--> {}", clusterId, forwardInfo, this);

        HttpEntity<DbCluster> entity = new HttpEntity<>(null, headers);
        restTemplate.exchange(appendDcIdAndOperator(changeClusterPath, dcId, operator), HttpMethod.POST, entity, String.class, clusterId);

    }

    @Override
    public void clusterModified(String clusterId, ForwardInfo forwardInfo, String operator) {

        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
        logger.info("[clusterModified][forward]{},{} --> {}", clusterId, forwardInfo, this);

        HttpEntity<DbCluster> entity = new HttpEntity<>(null, headers);
        restTemplate.exchange(appendOperator(operator), HttpMethod.PUT, entity, String.class, clusterId);

    }

    @Override
    public void clusterDeleted(String clusterId, ForwardInfo forwardInfo, String operator) {

        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
        logger.info("[clusterDeleted][forward]{},{} --> {}", clusterId, forwardInfo, this);

        HttpEntity<DbCluster> entity = new HttpEntity<>(headers);
        restTemplate.exchange(appendOperator(operator), HttpMethod.DELETE, entity, String.class, clusterId);
    }

    private String appendOperator(String operator) {
        return changeClusterPath + "?operator=" + operator;
    }

    @Override
    public void updateUpstream(String clusterId, String backupClusterId, String ip, int port, ForwardInfo forwardInfo) {

        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.UPSTREAM_CHANGE.getForwardType());
        logger.info("[updateUpstream][forward]{},{},{}:{}, {}--> {}", clusterId, backupClusterId, ip, port, forwardInfo, this);

        HttpEntity<DbCluster> entity = new HttpEntity<>(headers);
        restTemplate.exchange(upstreamChangePath, HttpMethod.PUT, entity, String.class, backupClusterId, clusterId, ip, port);
    }

    @Override
    public Replicator getActiveReplicator(String clusterId, ForwardInfo forwardInfo) {
        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
        logger.info("[getActiveReplicator][forward]{},{} --> {}", clusterId, forwardInfo, this);

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        ResponseEntity<Replicator> response = restTemplate.exchange(getActiveReplicatorPath, HttpMethod.GET, entity, Replicator.class, clusterId);
        return response.getBody();
    }

    @Override
    public Endpoint getActiveMySQL(String clusterId, ForwardInfo forwardInfo) {
        HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
        logger.info("[getActiveMySQL][forward]{},{} --> {}", clusterId, forwardInfo, this);

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        ResponseEntity<DefaultEndPoint> response = restTemplate.exchange(getActiveMySQLPath, HttpMethod.GET, entity, DefaultEndPoint.class, clusterId);
        return response.getBody();
    }

    private HttpHeaders checkCircularAndGetHttpHeaders(ForwardInfo forwardInfo, ForwardType forwardType) {

        checkCircular(forwardInfo);

        if(forwardInfo == null){
            forwardInfo = new ForwardInfo(forwardType);
        }else{
            forwardInfo.setType(forwardType);
        }
        forwardInfo.addForwardServers(getCurrentServerId());

        HttpHeaders headers = new HttpHeaders();
        headers.add(ClusterManagerService.HTTP_HEADER_FOWRARD, Codec.DEFAULT.encode(forwardInfo));
        return headers;
    }

    private HttpHeaders checkCircularAndGetHttpHeaders(ForwardInfo forwardInfo) {

        return checkCircularAndGetHttpHeaders(forwardInfo, ForwardType.FORWARD);
    }

    private void checkCircular(ForwardInfo forwardInfo) {
        if(forwardInfo != null && forwardInfo.hasServer(getCurrentServerId())){
            throw new CircularForwardException(String.format("forwardinfo:%s, currentServer:%d", forwardInfo, getCurrentServerId()));
        }
    }

    @Override
    public String getCurrentMeta() {
        return null;
    }

}
