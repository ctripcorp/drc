package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.schedule.SyncDbInfoTask;
import com.ctrip.framework.drc.console.service.OpenApiService;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.vo.api.DbTableDrcRegionInfo;
import com.ctrip.framework.drc.console.vo.api.SingleDbTableDrcCheckResponse;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;


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

    @GetMapping("info/db/table")
    public ApiResult<SingleDbTableDrcCheckResponse> queryDrcDbTableInfos(@RequestParam String dbName,
                                                                         @RequestParam String tableName) {
        try {
            if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
                return ApiResult.getFailInstance(null, "dbName or tableName should not be empty");
            }
            DbTableDrcRegionInfo exactDrcDbInfos = openApiService.getDbTableDrcRegionInfos(dbName.toLowerCase(), tableName.toLowerCase());
            SingleDbTableDrcCheckResponse response = new SingleDbTableDrcCheckResponse(exactDrcDbInfos);
            return ApiResult.getSuccessInstance(response);
        } catch (Exception e) {
            logger.error("error in queryDrcDbTableInfos", e);
            return ApiResult.getFailInstance(null, "Please contact dev, unknown exception: " + e.getMessage());
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
    @PostMapping("/tmp/parseConfigFileGtidContent")
    public ApiResult parseConfigFileGtidContent(@RequestBody ConfigReq req) {
        try {
            return ApiResult.getSuccessInstance(mhaReplicationServiceV2.parseConfigFileGtidContent(req.getConfigText()));
        } catch (Exception e) {
            logger.error("error in syncDbInfo",e);
            return ApiResult.getFailInstance(e,"error");
        }
    }


    static class ConfigReq {
        private String configText;
        private Boolean update;

        public Boolean getUpdate() {
            return update;
        }

        public void setUpdate(Boolean update) {
            this.update = update;
        }

        public String getConfigText() {
            return configText;
        }

        public void setConfigText(String configText) {
            this.configText = configText;
        }
    }
}
