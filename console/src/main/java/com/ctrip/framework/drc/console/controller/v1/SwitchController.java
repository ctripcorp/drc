package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import static com.ctrip.framework.drc.console.service.broadcast.HttpNotificationBroadCast.NEED_BROADCAST;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-04
 */
@RestController
@RequestMapping("/api/drc/v1/switch/")
public class SwitchController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private SwitchService switchService;

    @RequestMapping(value = "clusters/{clusterId}/dbs/master", method = RequestMethod.PUT)
    public ApiResult notifyMasterDb(
            @RequestParam(defaultValue = NEED_BROADCAST) String broadcast,
            @PathVariable String clusterId,
            @RequestBody(required = false) String endpoint) {
        try {
            boolean broadcastFlag = Boolean.parseBoolean(broadcast);
            logger.info("[Notification] broadcast:{},{} master db {},",broadcastFlag, clusterId, endpoint);
            switchService.switchUpdateDb(
                    clusterId,
                    endpoint,
                    broadcastFlag
            );
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }catch (Exception e) {
            logger.error("[Notification] notify master db error", e);
            return ApiResult.getFailInstance(Boolean.FALSE);
        }

    }

    /**
     curl -H "Content-Type:application/json" -X PUT -d "127.0.0.1:1234" 'http://127.0.0.1:8080/api/drc/v1/switch/clusters/testCluster.testMha/replicators/master'
     */
    @SuppressWarnings("unchecked")
    @RequestMapping(value = "clusters/{clusterId}/replicators/master", method = RequestMethod.PUT)
    public ApiResult<Boolean> notifyMasterReplicator(
            @RequestParam(defaultValue = NEED_BROADCAST) String broadcast,
            @PathVariable String clusterId,
            @RequestBody(required = false) String endpoint) {
        try {
            boolean broadcastFlag = Boolean.parseBoolean(broadcast);
            logger.info("[Notification]broadcast:{}, {} master replicator {},", broadcast,clusterId, endpoint);
            switchService.switchListenReplicator(clusterId, endpoint, broadcastFlag);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }catch (Exception e){
            logger.error("[Notification] notify master replicator error", e);
            return ApiResult.getFailInstance(Boolean.FALSE);
        }
    }


    @RequestMapping(value = "clusters/dbs/master", method = RequestMethod.PUT)
    public ApiResult notifyMasterDb(@RequestBody ClusterConfigDto clusterConfigDto) {
        try {
            logger.info("switchUpdateDb broadcast: {}, clusters: {}", clusterConfigDto.isFirstHand(), clusterConfigDto.getClusterMap());
            switchService.switchUpdateDb(clusterConfigDto);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }catch (Exception e) {
            logger.error("[Notification] notify master db error", e);
            return ApiResult.getFailInstance(Boolean.FALSE);
        }

    }

    @RequestMapping(value = "clusters/replicators/master", method = RequestMethod.PUT)
    public ApiResult notifyMasterReplicator(@RequestBody ClusterConfigDto clusterConfigDto) {
        try {
            logger.info("switchListenReplicator broadcast: {}, clusters: {}", clusterConfigDto.isFirstHand(), clusterConfigDto.getClusterMap());
            switchService.switchListenReplicator(clusterConfigDto);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }catch (Exception e) {
            logger.error("[Notification] notify master replicator error", e);
            return ApiResult.getFailInstance(Boolean.FALSE);
        }

    }
}
