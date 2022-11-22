package com.ctrip.framework.drc.console.controller.monitor;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.request.MonitorSwitchRequest;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jixinwang on 2020/12/29
 */
@RestController
@RequestMapping("/api/drc/v1/monitor/")
public class MonitorController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorService monitorService;

    private MachineTblDao machineTblDao = DalUtils.getInstance().getMachineTblDao();

    @PostMapping("switches/{status}")
    public ApiResult switchMonitors(@RequestBody MonitorSwitchRequest monitorSwitchRequest, @PathVariable String status) {

        try {
            monitorService.switchMonitors(monitorSwitchRequest.getMhaGroupIds(), status);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }

    @PostMapping("switch/{mhaName}/{status}")
    public ApiResult switchMonitors(@PathVariable String mhaName, @PathVariable String status) {
        try {
            monitorService.switchMonitors(mhaName, status);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }

    @GetMapping("switches/on")
    public ApiResult getMhaNamesToBeMonitored() {

        try {
            List<String> mhaNamesToBeMonitored = monitorService.queryMhaNamesToBeMonitored();
            return ApiResult.getSuccessInstance(mhaNamesToBeMonitored);
        } catch (Exception e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }
    
    @GetMapping("uuid")
    public ApiResult getUuidFromDBForRemoteDC(@RequestParam(value = "ip", required = false) String ip, @RequestParam(value = "port", required = false) int port) {
        try {
            MachineTbl machineTbl = null;
            machineTbl = machineTblDao.queryByIpPort(ip, port);
            if (machineTbl == null) {
                return ApiResult.getFailInstance(null);
            }
            return ApiResult.getSuccessInstance(machineTbl);
        } catch (SQLException e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }
    
    @PostMapping("uuid")
    public ApiResult UpdateUuidToDBForRemoteDC(@RequestBody MachineTbl machineTbl) {
        try {
            int updateRows = machineTblDao.update(machineTbl);
            logger.info("[[monitor=UUIDMonitor]] id"+machineTbl.getId()+"ip:port"+machineTbl.getIp()+":" +machineTbl.getPort()+"uuid:"+machineTbl.getUuid());
            if (updateRows == 1) {
                return ApiResult.getSuccessInstance("success");
            }
            return ApiResult.getFailInstance(null);
        } catch (SQLException e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }
}
