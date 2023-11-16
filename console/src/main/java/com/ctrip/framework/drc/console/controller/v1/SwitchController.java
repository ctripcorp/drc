package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.service.impl.SwitchServiceImpl;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
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
    private SwitchServiceImpl switchService;

    @RequestMapping(value = "clusters/{clusterId}/dbs/master", method = RequestMethod.PUT)
    public ApiResult notifyMasterDb(@PathVariable String clusterId, @RequestBody(required = false) String endpoint) {
        logger.info("[Notification] {} master db {}", clusterId, endpoint);
        return switchService.switchUpdateDb(clusterId, new DbEndpointDto(endpoint.split(":")[0], Integer.parseInt(endpoint.split(":")[1])));
    }

    /**
     curl -H "Content-Type:application/json" -X PUT -d "127.0.0.1:1234" 'http://127.0.0.1:8080/api/drc/v1/switch/clusters/testCluster.testMha/replicators/master'
     */
    @RequestMapping(value = "clusters/{clusterId}/replicators/master", method = RequestMethod.PUT)
    public ApiResult<Boolean> notifyMasterReplicator(@PathVariable String clusterId, @RequestBody(required = false) String endpoint) {
        logger.info("[Notification] {} master replicator {}", clusterId, endpoint);
        switchService.switchListenReplicator(clusterId, endpoint);
        return ApiResult.getSuccessInstance(Boolean.TRUE);
    }
}
