package com.ctrip.framework.drc.console.controller.v1;


import com.ctrip.framework.drc.console.service.impl.ConfigServiceImpl;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
@RestController
@RequestMapping("/api/drc/v1/")
public class ConfigController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConfigServiceImpl configService;


    @GetMapping("data/types")
    public ApiResult getAllDrcSupportDataType() {
        logger.info("[API] DRC support Data Type API is called once");
        return ApiResult.getSuccessInstance(configService.getAllDrcSupportDataTypes());
    }

    @GetMapping("health")
    public ApiResult getHealthStatus() {
        logger.info("[API] Health Status API is called once");
        return ApiResult.getSuccessInstance(Boolean.TRUE);
    }

}
