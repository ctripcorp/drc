package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.service.impl.DrcBuildServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceTwoImpl;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    private DrcBuildServiceImpl drcBuildService;


    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    
    
    @DeleteMapping(value = "routes/proxy")
    public ApiResult deleteProxyRoute(@RequestParam(value = "routeOrgName") String routeOrgName,
                                      @RequestParam(value = "srcDcName") String srcDcName,
                                      @RequestParam(value = "dstDcName") String dstDcName,
                                      @RequestParam(value = "tag") String tag) {
        logger.info("[meta] delete proxy route for {}-{},{}->{}", routeOrgName, tag, srcDcName, dstDcName);
        return drcMaintenanceService.deleteRoute(routeOrgName, srcDcName, dstDcName, tag);
    }

    @GetMapping(value = "routes")
    public ApiResult getProxyRoutes(@RequestParam(value = "routeOrgName", required = false) String routeOrgName,
                                    @RequestParam(value = "srcDcName", required = false) String srcDcName,
                                    @RequestParam(value = "dstDcName", required = false) String dstDcName,
                                    @RequestParam(value = "tag", required = false) String tag,
                                    @RequestParam(value = "deleted",required = true) Integer deleted){
        logger.info("[meta] get proxy routes for {}-{},{}->{},{}", routeOrgName, tag, srcDcName, dstDcName,deleted);
        List<RouteDto> routeDtoList = metaInfoService.getRoutes(routeOrgName, srcDcName, dstDcName, tag,deleted);
        return ApiResult.getSuccessInstance(routeDtoList);
    }

    @PostMapping(value = "routes")
    public ApiResult submitProxyRouteConfig(@RequestBody RouteDto routeDto) {
        logger.info("[meta] submit proxy route config for {}", routeDto);
        return ApiResult.getSuccessInstance(drcBuildService.submitProxyRouteConfig(routeDto));
    }

    @PostMapping("dcs/{dc}")
    public ApiResult inputDc(@PathVariable String dc) {
        logger.info("[meta] input dc {}", dc);
        return drcMaintenanceService.inputDc(dc);
    }

    @PostMapping("orgs/{org}")
    public ApiResult inputBu(@PathVariable String org) {
        logger.info("[meta] input bu {}", org);
        return drcMaintenanceService.inputBu(org);
    }

    @PostMapping("proxy")
    public ApiResult inputProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] load proxy: {}", proxyDto);
        return drcMaintenanceService.inputProxy(proxyDto);
    }

    @DeleteMapping("proxy")
    public ApiResult deleteProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] delete proxy: {}", proxyDto);
        return drcMaintenanceService.deleteProxy(proxyDto);
    }

    @GetMapping("proxy/uris/dcs/{dc}")
    public ApiResult getProxyUris(@PathVariable String dc) {
        logger.info("[meta] get proxy uris for {}", dc);
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceTwo.getProxyUris(dc));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

    @GetMapping("proxy/uris")
    public ApiResult getAllProxyUris() {
        logger.info("[meta] get all proxy uris");
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceTwo.getAllProxyUris());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }
    

    
}
