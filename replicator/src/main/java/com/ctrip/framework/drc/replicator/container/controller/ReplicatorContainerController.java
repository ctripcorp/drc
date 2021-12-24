package com.ctrip.framework.drc.replicator.container.controller;

import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.driver.command.packet.ResultCode.SERVER_ALREADY_EXIST;

/**
 * Created by mingdongli
 * 2019/10/30 上午9:50.
 */
@RestController
@RequestMapping("/replicators")
public class ReplicatorContainerController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ServerContainer<ReplicatorConfig, ApiResult> serverContainer;

    private ExecutorService replicatorExecutorService = ThreadUtils.newFixedThreadPool(16, "ReplicatorContainerController");

    /**
     *curl -H "Content-Type:application/json" -X PUT -d '{
     *     "clusterName": "bbztriptrackShardBaseDB_dalcluster_monitor",
     *     "gtidSet":"266e5755-124e-11ea-9b7b-98039bbedf9c:366561-370331:370732-374168,560f4cad-8c39-11e9-b53b-6c92bf463216:1-2051271042,c372080a-1804-11ea-8add-98039bbedf9c:1-249875,de76bd16-d524-11e9-8ce8-98039b97d08e:1,f360f941-2793-11ea-a239-98039bbedf9c:1-6565:6575-698146",
     *     "applierPort":8383,
     *     "readUser":"m_drc_r",
     *     "readPassward":"g?gwWrlwiXwiqzk1pp2d",
     *     "status":0,
     *     "master": {
     *         "ip": "10.28.198.36",
     *         "port": 55944,
     *         "master" : true
     *     },
     *     "uuids": ["66e5755-124e-11ea-9b7b-98039bbedf9c","560f4cad-8c39-11e9-b53b-6c92bf463216","c372080a-1804-11ea-8add-98039bbedf9c","de76bd16-d524-11e9-8ce8-98039b97d08e","f360f941-2793-11ea-a239-98039bbedf9c"]
     * }' 'http://127.0.0.1:8080/replicators'
     * @param replicatorConfigDto
     * @return
     */
    @RequestMapping(method = RequestMethod.PUT)
    public ApiResult<Boolean> start(@RequestBody ReplicatorConfigDto replicatorConfigDto) {
        try {
            logger.info("[Start] replicator instance with {}", replicatorConfigDto);
            ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
            String registryKey = replicatorConfig.getRegistryKey();
            InstanceStatus instanceStatus = InstanceStatus.getInstanceStatus(replicatorConfig.getStatus());
            logger.info("[Add] {} instance {}", instanceStatus, registryKey);

            Endpoint upstreamMaster = serverContainer.getUpstreamMaster(registryKey);
            Endpoint currentUpstreamMaster = replicatorConfig.getEndpoint();
            logger.info("[upstreamMaster] is {}, current is {}", upstreamMaster, currentUpstreamMaster);
            if (upstreamMaster != null && upstreamMaster.equals(replicatorConfig.getEndpoint())) {
                logger.info("[Add] duplicate {} instance {}", instanceStatus, registryKey);
                return ApiResult.getInstance(Boolean.FALSE, SERVER_ALREADY_EXIST.getCode(), SERVER_ALREADY_EXIST.getMessage());
            }

            replicatorExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        serverContainer.removeServer(registryKey, false);
                        serverContainer.addServer(replicatorConfig);
                    } catch (Exception e) {
                        logger.error("start error", e);
                    }
                }
            });
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("init error", t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(method = RequestMethod.POST)
    public ApiResult<Boolean> restart(@RequestBody ReplicatorConfigDto replicatorConfigDto) {

        ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
        String registryKey = replicatorConfig.getRegistryKey();
        try {
            logger.info("[Restart] {} with config {}", registryKey, replicatorConfigDto);
            replicatorExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    serverContainer.removeServer(registryKey, false);
                    serverContainer.addServer(replicatorConfig);
                }
            });
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("[Restart] error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/register", method = RequestMethod.PUT)
    public ApiResult<Boolean> register(@RequestBody ReplicatorConfigDto replicatorConfigDto) {

        try {
            logger.info("[Register] replicator instance with {}", replicatorConfigDto);
            ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
            String registryPath = replicatorConfig.getRegistryKey();
            return serverContainer.register(registryPath, replicatorConfig.getApplierPort());
        } catch (Throwable t) {
            logger.error("register error", t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/{registryKey}/", method = RequestMethod.DELETE)
    public void destroy(@PathVariable String registryKey) {
        logger.info("[Remove] replicator registryKey {}", registryKey);
        serverContainer.removeServer(registryKey, true);
    }

}
