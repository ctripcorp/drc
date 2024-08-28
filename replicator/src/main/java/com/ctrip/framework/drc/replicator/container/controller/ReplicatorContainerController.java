package com.ctrip.framework.drc.replicator.container.controller;

import com.ctrip.framework.drc.core.concurrent.DrcKeyedOneThreadTaskExecutor;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorDetailInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.container.controller.task.AddKeyedTask;
import com.ctrip.framework.drc.replicator.container.controller.task.DeleteKeyedTask;
import com.ctrip.framework.drc.replicator.container.controller.task.RegisterKeyedTask;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
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

    private ExecutorService notifyExecutorService = ThreadUtils.newFixedThreadPool(PROCESSORS_SIZE, "Keyed-Task-Service");
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
            logger.info("[Receive][Start] replicator instance({}) with {}", registryKey, replicatorConfigDto);
            notifyExecutor.execute(registryKey, new AddKeyedTask(registryKey, replicatorConfig, serverContainer));
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
            logger.info("[Receive][Restart] replicator instance({}) with config {}", registryKey, replicatorConfigDto);
            notifyExecutor.execute(registryKey, new AddKeyedTask(registryKey, replicatorConfig, serverContainer));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("[Restart] error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/register", method = RequestMethod.PUT)
    public ApiResult<Boolean> register(@RequestBody ReplicatorConfigDto replicatorConfigDto) {
        ReplicatorConfig replicatorConfig = replicatorConfigDto.toReplicatorConfig();
        String registryKey = replicatorConfig.getRegistryKey();
        try {
            logger.info("[Receive][Register] replicator instance with {}", replicatorConfig);
            notifyExecutor.execute(registryKey, new RegisterKeyedTask(registryKey, replicatorConfig, serverContainer));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("register error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/{registryKey}/", method = RequestMethod.DELETE)
    public ApiResult<Boolean> destroy(@PathVariable String registryKey) {
        try {
            logger.info("[Receive][Remove] replicator registryKey {}", registryKey);
            notifyExecutor.execute(registryKey, new DeleteKeyedTask(registryKey, null, serverContainer));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("destroy error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
        
    }

    @RequestMapping(value = "/info/{registryKey}", method = RequestMethod.GET)
    public ApiResult<ReplicatorDetailInfoDto> info(@PathVariable String registryKey) {
        try {
            return serverContainer.getInfo(registryKey);
        } catch (Throwable t) {
            logger.error("get info error for {}", registryKey, t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/info/all", method = RequestMethod.GET)
    public ApiResult<List<ReplicatorInfoDto>> info() {
        try {
            return serverContainer.getInfo();
        } catch (Throwable t) {
            logger.error("get info error", t);
            return ApiResult.getFailInstance(null);
        }
    }

}
