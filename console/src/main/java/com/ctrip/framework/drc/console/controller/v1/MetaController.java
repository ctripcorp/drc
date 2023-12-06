package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.service.v2.resource.ProxyService;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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

    @DeleteMapping(value = "routes/proxy")
    public ApiResult deleteProxyRoute(@RequestParam(value = "routeOrgName") String routeOrgName,
                                      @RequestParam(value = "srcDcName") String srcDcName,
                                      @RequestParam(value = "dstDcName") String dstDcName,
                                      @RequestParam(value = "tag") String tag) {
        logger.info("[meta] delete proxy route for {}-{},{}->{}", routeOrgName, tag, srcDcName, dstDcName);
        return routeService.deleteRoute(routeOrgName, srcDcName, dstDcName, tag);
    }

    @GetMapping(value = "routes")
    public ApiResult getProxyRoutes(@RequestParam(value = "routeOrgName", required = false) String routeOrgName,
                                    @RequestParam(value = "srcDcName", required = false) String srcDcName,
                                    @RequestParam(value = "dstDcName", required = false) String dstDcName,
                                    @RequestParam(value = "tag", required = false) String tag,
                                    @RequestParam(value = "deleted", required = true) Integer deleted) {
        logger.info("[meta] get proxy routes for {}-{},{}->{},{}", routeOrgName, tag, srcDcName, dstDcName, deleted);
        List<RouteDto> routeDtoList = routeService.getRoutes(routeOrgName, srcDcName, dstDcName, tag, deleted);
        return ApiResult.getSuccessInstance(routeDtoList);
    }

    @PostMapping(value = "routes")
    public ApiResult submitProxyRouteConfig(@RequestBody RouteDto routeDto) {
        logger.info("[meta] submit proxy route config for {}", routeDto);
        return ApiResult.getSuccessInstance(routeService.submitProxyRouteConfig(routeDto));
    }

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

    @PostMapping("proxy")
    public ApiResult inputProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] load proxy: {}", proxyDto);
        return proxyService.inputProxy(proxyDto);
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
            return ApiResult.getSuccessInstance(proxyService.getAllProxyUris());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }


}
