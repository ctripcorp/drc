package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerService;
import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedList;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class AbstractDispatcherClusterManagerController extends AbstractController {

    protected static final String MODEL_META_SERVER = "MODEL_META_SERVER";

    @Autowired
    public ClusterManager currentMetaServer;

    @Autowired
    private SlotManager slotManager;

    @Autowired
    public ClusterServers<ClusterManager> servers;

    @ModelAttribute
    public void populateModel(@PathVariable final String clusterId, @RequestHeader(name = ClusterManagerService.HTTP_HEADER_FOWRARD, required = false) ForwardInfo forwardInfo, Model model, HttpServletRequest request) {

        if (forwardInfo != null) {
            logger.info("[populateModel]{},{}", clusterId, forwardInfo);
        }
        ClusterManager metaServer = getClusterManager(clusterId, forwardInfo, request.getRequestURI());
        if (metaServer == null) {
            throw new RuntimeException("UnfoundAliveSererException");
        }
        model.addAttribute(MODEL_META_SERVER, metaServer);
        if (forwardInfo != null) {
            model.addAttribute(forwardInfo);
        }
    }

    private ClusterManager getClusterManager(String clusterId, ForwardInfo forwardInfo, String uri) {

        int slotId = slotManager.getSlotIdByKey(clusterId);
        SlotInfo slotInfo = slotManager.getSlotInfo(slotId);

        if (forwardInfo != null && forwardInfo.getType() == ForwardType.MOVING) {

            if (!(slotInfo.getSlotState() == SLOT_STATE.MOVING && slotInfo.getToServerId().equalsIgnoreCase(currentMetaServer.getServerId()))) {
                throw new RuntimeException("MovingTargetException");
            }
            logger.info("[getClusterManager][use current server]");
            return currentMetaServer;
        }

        if (forwardInfo != null && forwardInfo.getType() == ForwardType.MULTICASTING) {
            logger.info("[multicast message][do now]");
            return currentMetaServer;
        }

        META_SERVER_SERVICE service = META_SERVER_SERVICE.fromPath(uri);
        String serverId = slotManager.getServerIdByKey(clusterId);
        logger.info("slotId:{}, clusterId:{}, slotInfo:{}, serverId:{}", slotId, clusterId, slotInfo, serverId);

        if (serverId == null) {
            throw new IllegalStateException("clusterId:" + clusterId + ", unfound server");
        }

        if(service.getForwardType() == ForwardType.MULTICASTING){

            logger.info("[getMetaServer][multi casting]{}, {}, {}", clusterId, forwardInfo, uri);
            Set<ClusterManager> allServers =  servers.allClusterServers();
            ClusterManager current = servers.getClusterServer(serverId);
            allServers.remove(current);

            return MultiMetaServer.newProxy(current, new LinkedList<>(allServers));
        } else if (service.getForwardType() == ForwardType.FORWARD) {

            return servers.getClusterServer(serverId);
        } else {
            throw new IllegalStateException("service type can not be:" + service.getForwardType());
        }
    }

}
