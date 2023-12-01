package com.ctrip.framework.drc.applier.container.controller;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.utils.NameUtils;
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
            serverContainer.addServer(config);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Throwable t){
            logger.error("doAddServer error for {}", config.getRegistryKey(), t);
            return ApiResult.getFailInstance(t);
        }
    }

    @RequestMapping(value = "/register", method = RequestMethod.PUT)
    public ApiResult<Boolean> register(@RequestBody ApplierConfigDto config) {

        try {
            logger.info("[http] register applier: " + config);
            String registryKey = config.getRegistryKey();
            registryKey = NameUtils.dotSchemaIfNeed(registryKey, config.getApplyMode(), config.getNameFilter());
            serverContainer.registerServer(registryKey);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            logger.error("register error", t);
            return ApiResult.getFailInstance(false);
        }
    }

    @RequestMapping(value = {"/{registryKey}/", "/{registryKey}/{delete}"}, method = RequestMethod.DELETE)
    public ApiResult<Boolean> remove(@PathVariable String registryKey, @PathVariable Optional<Boolean> delete) {
        logger.info("[Remove] applier registryKey {}", registryKey);
        try {
            boolean deleted = true;
            if (delete.isPresent()) {
                deleted = delete.get();
                logger.info("[delete] is updated to {} for {}", deleted, registryKey);
            }
            serverContainer.removeServer(registryKey, deleted);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("remove error", e);
            return ApiResult.getFailInstance(false);
        }
    }

}
