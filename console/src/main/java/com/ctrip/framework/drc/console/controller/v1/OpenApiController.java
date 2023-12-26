package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.schedule.SyncDbInfoTask;
import com.ctrip.framework.drc.console.service.OpenApiService;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName OpenApiController
 * @Author haodongPan
 * @Date 2022/1/4 17:49
 * @Version: $
 * 
 */
@RestController
@RequestMapping("/api/drc/v1/openapi/")
public class OpenApiController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private OpenApiService openApiService;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private SyncDbInfoTask syncDbInfoTask;
    

    @GetMapping("info/messengers")
    public ApiResult getAllMessengersInfo() {
        try {
            return ApiResult.getSuccessInstance(openApiService.getAllMessengersInfo());
        } catch (Exception e) {
            logger.error("error in getAllMessengersInfo",e);
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("info/dbs")
    public ApiResult queryDrcDbInfos(@RequestParam(required=false) String dbName) {
        try {
            return ApiResult.getSuccessInstance(openApiService.getDrcDbInfos(dbName));
        } catch (Exception e) {
            logger.error("error in queryDrcDbInfos",e);
            return ApiResult.getFailInstance(e,"error");
        }
    }
    
    @DeleteMapping("/tmp/duplicateDbs")
    public ApiResult deleteDuplicateDbs(@RequestParam boolean executeDelete) {
        try {
            return ApiResult.getSuccessInstance(mhaDbMappingService.removeDuplicateDbTblWithoutMhaDbMapping(executeDelete));
        } catch (Exception e) {
            logger.error("error in deleteDuplicateDbs",e);
            return ApiResult.getFailInstance(e,"error");
        }
    }
    
    @PostMapping("/tmp/syncDbInfo")
    public ApiResult syncDbInfo() {
        try {
            return ApiResult.getSuccessInstance(syncDbInfoTask.call());
        } catch (Exception e) {
            logger.error("error in syncDbInfo",e);
            return ApiResult.getFailInstance(e,"error");
        }
    }
    
    
}
