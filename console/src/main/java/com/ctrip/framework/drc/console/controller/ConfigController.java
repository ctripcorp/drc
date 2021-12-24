package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.ConfigServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    /**
     curl -H "Content-Type:application/json" -X POST -d '{
     "ip": "10.2.83.109",
     "port": 3306
     }' 'http://127.0.0.1:8080/api/drc/v1/mhas/{mhaName}/instances'
     */
    @RequestMapping(value = "mhas/{mhaName}/instances", method = RequestMethod.POST)
    public ApiResult notifyMasterDb(@PathVariable String mhaName, @RequestBody(required = false) DefaultEndPoint endPoint) {
        String ip = endPoint.getIp();
        int port = endPoint.getPort();
        logger.info("[Notification] {} master db {}:{}", mhaName, ip, port);
        return drcMaintenanceService.changeMasterDb(mhaName, ip, port);
    }

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

    @GetMapping("unit/result/hickwall")
    public ApiResult getUnitResultHickwallAddress() {
        logger.info("[API] unit result hickwall address");
        return ApiResult.getSuccessInstance(monitorTableSourceProvider.getUnitResultHickwallAddress());
    }
}
