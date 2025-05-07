package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.service.v2.resource.ProxyService;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
@RestController
@RequestMapping("/api/drc/v1/meta/")
public class MetaController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RouteService routeService;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    @PostMapping("dcs/{dc}")
    public ApiResult inputDc(@PathVariable String dc) {
        logger.info("[meta] input dc {}", dc);
        return proxyService.inputDc(dc);
    }

    @PostMapping("orgs/{org}")
    public ApiResult inputBu(@PathVariable String org) {
        logger.info("[meta] input bu {}", org);
        return proxyService.inputBu(org);
    }

    @DeleteMapping("proxy")
    public ApiResult deleteProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] delete proxy: {}", proxyDto);
        return proxyService.deleteProxy(proxyDto);
    }

    @GetMapping("proxy/uris/dcs/{dc}")
    public ApiResult getProxyUris(@PathVariable String dc) {
        logger.info("[meta] get proxy uris for {}", dc);
        try {
            return ApiResult.getSuccessInstance(proxyService.getProxyUris(dc));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

    @GetMapping("proxy/uris")
    public ApiResult getAllProxyUris() {
        logger.info("[meta] get all proxy uris");
        try {
            return ApiResult.getSuccessInstance(proxyService.getRelayProxyUris());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

    @GetMapping("panelUrl")
    public ApiResult getPanelUrl() {
        try {
            return ApiResult.getSuccessInstance(defaultConsoleConfig.getConsolePanelUrl());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }


}
