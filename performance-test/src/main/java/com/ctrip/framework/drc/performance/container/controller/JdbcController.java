package com.ctrip.framework.drc.performance.container.controller;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.performance.container.JdbcTestServerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

/**
 * Created by jixinwang on 2021/9/9
 */
@RestController
@RequestMapping("/appliers/jdbc")
public class JdbcController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private JdbcTestServerContainer serverContainer = new JdbcTestServerContainer();

    @RequestMapping(method = RequestMethod.POST)
    public ApiResult post(@RequestBody ApplierConfigDto config) {
        logger.info("[http] post jdbc: " + config);
        try {
            return ApiResult.getSuccessInstance(serverContainer.addServer(config));
        } catch (Exception e) {
            logger.error("[http] post jdbc: ", e);
            return ApiResult.getFailInstance(false);
        }
    }

    @RequestMapping(value = {"/{registryKey:.+}"}, method = RequestMethod.DELETE)
    public ApiResult<Boolean> remove(@PathVariable String registryKey) {
        logger.info("[Remove] applier registryKey {}", registryKey);
        try {
            serverContainer.removeServer(registryKey, false);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("remove error", e);
            return ApiResult.getFailInstance(false);
        }
    }

}
