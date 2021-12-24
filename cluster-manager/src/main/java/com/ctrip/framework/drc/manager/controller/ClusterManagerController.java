package com.ctrip.framework.drc.manager.controller;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.manager.healthcheck.service.ClusterService;
import com.ctrip.framework.drc.manager.healthcheck.service.DefaultClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * http://localhost:8080/clusters/health
 * Created by mingdongli
 * 2019/10/30 上午9:50.
 */
@RestController
@RequestMapping("/clusters")
public class ClusterManagerController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ClusterService clusterService = DefaultClusterService.getInstance();

    @RequestMapping(value = "health", method = RequestMethod.GET)
    public String healthCheck() {
        return "ok";
    }

    /**
     * curl -H "Content-Type:application/json" -X PUT -d '{
     *     "name": "clusterName1_JQ_8383",
     *     "replicator": {
     *         "ip" :"127.0.0.1",
     *         "port":8080,
     *         "applierPort":8383,
     *         "gtidSkip":""
     *     },
     *     "dbs": [{
     *             "username": "root",
     *             "password": "root",
     *             "ip": "127.0.0.1",
     *             "port": 3306,
     *             "master" : true
     *         },{
     *             "username": "root",
     *             "password": "root",
     *             "ip": "127.0.0.1",
     *             "port": 3307,
     *             "master" : false
     *         }],
     *     "appliers": [{
     *             "ip": "127.0.0.1",
     *             "port": 8080,
     *             "targetIdc" : "oy"
     *         },{
     *             "ip": "127.0.0.1",
     *             "port": 8080,
     *             "targetIdc" : "fq"
     *         }]
     * }' 'http://127.0.0.1:8081/clusters/dbclusters'
     * @param dbCluster
     */
    @RequestMapping(value = "/dbclusters", method = RequestMethod.PUT)
    public ApiResult<Boolean> add(@RequestBody DbCluster dbCluster) {
        try {
            logger.info("[Add] {}", dbCluster);
            clusterService.addDbCluster(dbCluster);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

}
