package com.ctrip.framework.drc.console.controller.monitor;

import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.ConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyCheckTestConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.service.checker.UserAccessChecker;
import com.ctrip.framework.drc.console.service.monitor.ConsistencyMonitorService;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2021/8/2
 */
@RestController
@RequestMapping("/api/drc/v1/monitor/")
public class ConsistencyMonitorController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private ConsistencyMonitorService consistencyMonitorService;

    private UserAccessChecker userAccessChecker;

    public ConsistencyMonitorController(ConsistencyMonitorService consistencyMonitorService, UserAccessChecker userAccessChecker) {
        this.consistencyMonitorService = consistencyMonitorService;
        this.userAccessChecker = userAccessChecker;
    }

    @PostMapping("consistency/data/{mhaA}/{mhaB}")
    public ApiResult addDataConsistencyMonitor(@RequestBody DelayMonitorConfig delayMonitorConfig, @PathVariable String mhaA, @PathVariable String mhaB) {
        logger.info("[Monitor] add data consistency, mhaA is {}, mhaB is {}, delayMonitorConfig is {}", mhaA, mhaB, delayMonitorConfig);
        try {
            consistencyMonitorService.addDataConsistencyMonitor(mhaA, mhaB, delayMonitorConfig);
            return ApiResult.getSuccessInstance("success");
        } catch (SQLException e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("consistency/data/{mhaA}/{mhaB}")
    public ApiResult getDataConsistencyMonitor(@PathVariable String mhaA, @PathVariable String mhaB) {
        try {
            List<DataConsistencyMonitorTbl> result = consistencyMonitorService.getDataConsistencyMonitor(mhaA, mhaB);
            return ApiResult.getSuccessInstance(result);
        } catch (SQLException e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @DeleteMapping("consistency/data/{id}")
    public ApiResult deleteDataConsistencyMonitor(@PathVariable int id) {
        try {
            consistencyMonitorService.deleteDataConsistencyMonitor(id);
            return ApiResult.getSuccessInstance("success");
        } catch (SQLException e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            return ApiResult.getFailInstance(e);
        }
    }

    @PostMapping("consistency/switches/{status}")
    public ApiResult switchDataConsistencyMonitor(@RequestBody List<ConsistencyMonitorConfig> consistencyMonitorConfigs, @PathVariable String status) {
        return ApiResult.getSuccessInstance(consistencyMonitorService.switchDataConsistencyMonitor(consistencyMonitorConfigs, status));
    }

    @PostMapping("unit/switches/{status}")
    public ApiResult switchUnitVerification(@RequestBody MhaGroupPair mhaGroupPair, @PathVariable String status) {
        return ApiResult.getSuccessInstance(consistencyMonitorService.switchUnitVerification(mhaGroupPair, status));
    }

    @PostMapping("consistency/data/full/{mhaA}/{mhaB}")
    public ApiResult addFullDataConsistencyMonitor(@RequestBody FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig, @PathVariable String mhaA, @PathVariable String mhaB) {
        logger.info("[Monitor] add data consistency, mhaA is {}, mhaB is {}, fullDataConsistencyMonitorConfig is {}", mhaA, mhaB, fullDataConsistencyMonitorConfig);
        try {
            if (fullDataConsistencyMonitorConfig.getEndTimeStamp() == null) {
                return ApiResult.getFailInstance("end timestamp is null");
            }
            consistencyMonitorService.addFullDataConsistencyMonitor(fullDataConsistencyMonitorConfig);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            return ApiResult.getFailInstance(e);
        }
    }

    // just for internalDataTest
    @PostMapping("consistency/data/full/internalTest")
    public ApiResult addFullDataConsistencyCheckForTest(@RequestBody FullDataConsistencyCheckTestConfig fullDataConsistencyCheckTestConfig) {
        logger.info("[internalTest] add data consistency, fullDataConsistencyCheckTestConfig is {}", fullDataConsistencyCheckTestConfig);
        try {
            consistencyMonitorService.addFullDataConsistencyCheck(fullDataConsistencyCheckTestConfig);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            return ApiResult.getFailInstance(e);
        }

    }

    @GetMapping("consistency/data/full/checkStatusForTest/{schema}/{table}/{key}")
    public ApiResult getFullDataCheckStatusForTest(@PathVariable String schema, @PathVariable String table, @PathVariable String key) {
        logger.info("[internalTest] check data consistency status, config is {}", schema + "." + table + "." + key);
        try {
            Map<String, Object> result = consistencyMonitorService.getFullDataCheckStatusForTest(schema, table, key);
            return ApiResult.getSuccessInstance(result);
        } catch (SQLException e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("consistency/increment/history")
    public ApiResult getIncrementInconsistencyHistory(@RequestParam(value = "pageNo", required = true) int pageNo,
                                                      @RequestParam(value = "pageSize", required = true) int pageSize,
                                                      @RequestParam(value = "dbName", required = false) String dbName,
                                                      @RequestParam(value = "tableName", required = false) String tableName,
                                                      @RequestParam(value = "startTime", required = false) String startTime,
                                                      @RequestParam(value = "endTime", required = false) String endTime) {
        try {
            List<DataInconsistencyHistoryTbl> list = consistencyMonitorService.getIncrementInconsistencyHistory(pageNo, pageSize, dbName, tableName, startTime, endTime);
            return ApiResult.getSuccessInstance(list);
        } catch (SQLException e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("consistency/increment/history/count")
    public ApiResult getIncrementInconsistencyHistoryCount(@RequestParam(value = "dbName", required = false) String dbName,
                                                           @RequestParam(value = "tableName", required = false) String tableName,
                                                           @RequestParam(value = "startTime", required = false) String startTime,
                                                           @RequestParam(value = "endTime", required = false) String endTime) {
        try {
            long count = consistencyMonitorService.getIncrementInconsistencyHistoryCount(dbName, tableName, startTime, endTime);
            return ApiResult.getSuccessInstance(count);
        } catch (SQLException e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("consistency/increment/current")
    public ApiResult getCurrentIncrementInconsistencyRecord(@RequestParam long mhaGroupId, @RequestParam String dbName,
                                                            @RequestParam String tableName, @RequestParam String key,
                                                            @RequestParam String keyValue) {
        try {
            Map<String, Object> result = consistencyMonitorService.getCurrentIncrementInconsistencyRecord(mhaGroupId, dbName, tableName, key, keyValue);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("consistency/full/history/{mhaA}/{mhaB}/{dbName}/{tableName}/{key}/{checkTime}")
    public ApiResult getCurrentFullInconsistencyRecord(@PathVariable String mhaA, @PathVariable String mhaB, @PathVariable String dbName, @PathVariable String tableName, @PathVariable String key, @PathVariable String checkTime) {
        try {
            Map<String, Object> result = consistencyMonitorService.getCurrentFullInconsistencyRecord(mhaA, mhaB, dbName, tableName, key, checkTime);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            return ApiResult.getFailInstance(e);
        }
    }

    //actual Get, use post for RequestBody
    @PostMapping("consistency/full/historyForTest/{checkTime}")
    public ApiResult getCurrentFullInconsistencyRecordForTest(@RequestBody FullDataConsistencyCheckTestConfig testConfig, @PathVariable String checkTime) {
        try {
            Map<String, Object> result = consistencyMonitorService.getCurrentFullInconsistencyRecordForTest(testConfig, checkTime);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @PostMapping(value = "consistency/update")
    public ApiResult handleInconsistency(@RequestBody Map<String, String> updateInfo) {
        try {
            String userName = updateInfo.get("userName");
            // access check
            if (!userAccessChecker.isAllowed(userName)) {
                return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), "No operation permission!");
            }
            consistencyMonitorService.handleInconsistency(updateInfo);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }

    @DeleteMapping("consistency/history/ids/{id}")
    public ApiResult deleteDataInconsistency(@PathVariable String id) {
        String[] ids = id.split(",");
        Set<Long> idSet = Sets.newHashSet();
        for (String i : ids) {
            idSet.add(Long.parseLong(i));
        }
        return ApiResult.getSuccessInstance(consistencyMonitorService.deleteDataInconsistency(idSet));
    }
}
