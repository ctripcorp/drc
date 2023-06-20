package com.ctrip.framework.drc.replicator.container.controller;

import com.ctrip.framework.drc.core.concurrent.DrcKeyedOneThreadTaskExecutor;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

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

    private ExecutorService notifyExecutorService = ThreadUtils.newFixedThreadPool(PROCESSORS_SIZE, "Replicator-Start-Service");
    private KeyedOneThreadTaskExecutor<String> notifyExecutor = new DrcKeyedOneThreadTaskExecutor(notifyExecutorService);;

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
        ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
        String registryKey = replicatorConfig.getRegistryKey();
        try {
            logger.info("[Start] replicator instance({}) with {}", registryKey, replicatorConfigDto);
            notifyExecutor.execute(registryKey, new StartTask(registryKey, replicatorConfig));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("[Start] error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(method = RequestMethod.POST)
    public ApiResult<Boolean> restart(@RequestBody ReplicatorConfigDto replicatorConfigDto) {

        ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
        String registryKey = replicatorConfig.getRegistryKey();
        try {
            logger.info("[Restart] replicator instance({}) with config {}", registryKey, replicatorConfigDto);
            notifyExecutor.execute(registryKey, new StartTask(registryKey, replicatorConfig));
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

    class StartTask extends AbstractCommand {

        private String registryKey;

        private ReplicatorConfig replicatorConfig;

        public StartTask(String registryKey, ReplicatorConfig replicatorConfig) {
            this.registryKey = registryKey;
            this.replicatorConfig = replicatorConfig;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                if (checkConfig()) {
                    return;
                }
                removeOldInstance();
                addNewInstance();
            } catch (Exception e) {
                logger.error("Start] replicator instance({}) error", registryKey, e);
            }
        }

        private boolean checkConfig() {
            InstanceStatus instanceStatus = InstanceStatus.getInstanceStatus(replicatorConfig.getStatus());
            logger.info("[Start] replicator instance({}) with role: {}", registryKey, instanceStatus);
            Endpoint upstreamMaster = serverContainer.getUpstreamMaster(registryKey);
            Endpoint currentUpstreamMaster = replicatorConfig.getEndpoint();
            logger.info("[Start] replicator instance({}), upstreamMaster is {}, current is {}", registryKey, upstreamMaster, currentUpstreamMaster);
            if (upstreamMaster != null && upstreamMaster.equals(replicatorConfig.getEndpoint())) {
                logger.info("[Start] replicator instance({}), ignore duplicate instance, with role: {}", registryKey, instanceStatus);
                return false;
            }
            return true;
        }

        private void removeOldInstance() {
            logger.info("[Start] replicator instance({}), remove old instance", registryKey);
            long start = System.currentTimeMillis();
            serverContainer.removeServer(registryKey, false);
            logger.info("[Start] replicator instance{}, remove old instance cost: {}s", registryKey, (System.currentTimeMillis() - start) / 1000);
        }

        private void addNewInstance() {
            logger.info("[Start] replicator instance({}), add new instance", registryKey);
            long start = System.currentTimeMillis();
            serverContainer.addServer(replicatorConfig);
            logger.info("[Start] replicator instance({}), add new instance cost: {}s", registryKey, (System.currentTimeMillis() - start) / 1000);
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return registryKey;
        }
    }

}
