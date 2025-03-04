package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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

    @RequestMapping(value = "clusters/messengers/master", method = RequestMethod.PUT)
    public ApiResult notifyMasterMessenger(@RequestBody ClusterConfigDto clusterConfigDto) {
        try {
            logger.info("notifyMasterMessenger broadcast: {}, clusters: {}", clusterConfigDto.isFirstHand(), clusterConfigDto.getClusterMap());
            switchService.switchListenMessenger(clusterConfigDto);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }catch (Exception e) {
            logger.error("[Notification] notify master messenger error", e);
            return ApiResult.getFailInstance(Boolean.FALSE);
        }
    }
}
