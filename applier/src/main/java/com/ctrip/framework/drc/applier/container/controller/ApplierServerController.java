package com.ctrip.framework.drc.applier.container.controller;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

import com.ctrip.framework.drc.applier.activity.monitor.WatchActivity;
import com.ctrip.framework.drc.applier.activity.monitor.WatchActivity.LastLWM;
import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.container.controller.task.AddKeyedTask;
import com.ctrip.framework.drc.applier.container.controller.task.DeleteKeyedTask;
import com.ctrip.framework.drc.applier.container.controller.task.RegisterKeyedTask;
import com.ctrip.framework.drc.applier.container.controller.task.WatchKeyedTask;
import com.ctrip.framework.drc.applier.server.ApplierServer;
import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.concurrent.DrcKeyedOneThreadTaskExecutor;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

/**
 * @Author Slight
 * Nov 07, 2019
 */
@RestController
@RequestMapping("/appliers")
public class ApplierServerController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final ApplierDynamicConfig config = ApplierDynamicConfig.getInstance();
    private static final ExecutorService executorService = ThreadUtils.newThreadExecutor(PROCESSORS_SIZE,config.getApplierInstanceModifyThread(),"Applier-Task-Service");
    public static final KeyedOneThreadTaskExecutor<String> keyedExecutor = new DrcKeyedOneThreadTaskExecutor(executorService);

    @Autowired
    private ApplierServerContainer serverContainer;

    @RequestMapping(method = RequestMethod.PUT)
    public ApiResult put(@RequestBody ApplierConfigDto config) {
        logger.info("[http] put applier: " + config);
        return doAddServer(config);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ApiResult post(@RequestBody ApplierConfigDto config) {
        logger.info("[http] post applier: " + config);
        return doAddServer(config);
    }

    private ApiResult doAddServer(ApplierConfigDto config) {
        try {
            String registryKey = config.getRegistryKey();
            logger.info("[Receive][Add] applier: " + config);
            keyedExecutor.execute(config.getRegistryKey(),new AddKeyedTask(registryKey,config,serverContainer));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t){
            logger.error("doAddServer error for {}", config.getRegistryKey(), t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/register", method = RequestMethod.PUT)
    public ApiResult<Boolean> register(@RequestBody ApplierConfigDto config) {
        try {
            String registryKey = config.getRegistryKey();
            logger.info("[Receive][Register] applier: " + config);
            keyedExecutor.execute(config.getRegistryKey(),new RegisterKeyedTask(registryKey,config,serverContainer));
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t) {
            logger.error("register error", t);
            return ApiResult.getFailInstance(false);
        }
    }

    @RequestMapping(value = {"/{registryKey}/", "/{registryKey}/{delete}"}, method = RequestMethod.DELETE)
    public ApiResult<Boolean> remove(@PathVariable String registryKey, @PathVariable Optional<Boolean> delete) {
        
        try {
            logger.info("[Remove] applier registryKey {}", registryKey);
            boolean deleted = true;
            if (delete.isPresent()) {
                deleted = delete.get();
                logger.info("[delete] is updated to {} for {}", deleted, registryKey);
            }
            keyedExecutor.execute(registryKey, new DeleteKeyedTask(registryKey,null,serverContainer,deleted));
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("remove error", e);
            return ApiResult.getFailInstance(false);
        }
    }
    
    // todo hdpan only for test,should remove 
    @RequestMapping(value = "/{registryKey}/test/case/{code}", method = RequestMethod.POST)
    public ApiResult<Boolean> testWatch(@PathVariable String registryKey,@PathVariable Integer code) {
        try {
            ConcurrentHashMap<String, LastLWM> lastLWMHashMap = new ConcurrentHashMap<>();
            LastLWM mockLastLWM = new LastLWM(1,1,1);
            lastLWMHashMap.put(registryKey, mockLastLWM);
            
            
            DeleteKeyedTask deleteKeyedTask = new DeleteKeyedTask(registryKey, null, serverContainer, true);
            WatchKeyedTask watchKeyedTask = new WatchKeyedTask(registryKey, null, serverContainer, lastLWMHashMap);
            if (code == 1) {
                ApplierServer server = serverContainer.getServer(registryKey);
                server.setStatus(SystemStatus.STOPPED);
                logger.info("testWatch stop server:{}", server);
                keyedExecutor.execute(registryKey, deleteKeyedTask);
                keyedExecutor.execute(registryKey, watchKeyedTask);
            } else if (code == 2) {
                ApplierServer server = serverContainer.getServer(registryKey);
                server.setStatus(SystemStatus.STOPPED);
                logger.info("testWatch stop server:{}", server);
                keyedExecutor.execute(registryKey, watchKeyedTask);
                keyedExecutor.execute(registryKey, deleteKeyedTask);
            }
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("testWatch error", e);
            return ApiResult.getFailInstance(false);
        }
    }

}
