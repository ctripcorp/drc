package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @ClassName CentralServiceController
 * @Author haodongPan
 * @Date 2023/7/26 20:14
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/centralService/")
public class CentralServiceController {

    @Autowired
    private CentralService centralService;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @GetMapping("mhaTblV2s")
    public ApiResult getMhaTblV2s(@RequestParam String dcName) {
        try {
            logger.info("[[tag=centralService]] getMhaTblV2s");
            List<MhaTblV2> mhaTblV2s = centralService.getMhaTblV2s(dcName);
            return ApiResult.getSuccessInstance(mhaTblV2s);
        } catch (Throwable e) {
            logger.info("[[tag=centralService]] getMhaTblV2s fail");
            return ApiResult.getFailInstance(null, "getMhaTblV2s fail");
        }
    }

    @PostMapping("ddlHistory")
    public ApiResult<Integer> insertDdlHistory(@RequestBody DdlHistoryEntity requestBody) {
        try {
            logger.info("insertDdlHistory requestBody: {}", requestBody);
            return ApiResult.getSuccessInstance(centralService.insertDdlHistory(requestBody));
        } catch (Exception e) {
            logger.info("insertDdlHistory fail, requestBody: {}", requestBody, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mhaDbReplicationDtos")
    public ApiResult getMhaDbReplications(@RequestParam String dcName) {
        try {
            logger.info("[[tag=centralService]] getMhaDbReplications");
            List<MhaDbReplicationDto> mhaDbReplications = centralService.getMhaDbReplications(dcName);
            return ApiResult.getSuccessInstance(mhaDbReplications);
        } catch (Throwable e) {
            logger.info("[[tag=centralService]] getMhaDbReplications fail");
            return ApiResult.getFailInstance(null, "getMhaDbReplications fail");
        }
    }
    
    @GetMapping("uuid")
    public ApiResult getUuidInMetaDb(@RequestParam String mhaName,@RequestParam String ip,@RequestParam Integer port) {
        try {
            logger.info("[[tag=centralService]] getUuidInMetaDb");
            String uuid = centralService.getUuidInMetaDb(mhaName,ip,port);
            return ApiResult.getSuccessInstance(uuid);
        } catch (Throwable e) {
            logger.info("[[tag=centralService]] getUuidInMetaDb fail, mhaName: {}, ip: {}, port: {}", mhaName, ip, port, e);
            return ApiResult.getFailInstance(null, "getUuidInMetaDb fail");
        }
    }

    @PostMapping("uuid/correct")
    public ApiResult correctUuidInMetaDb(MachineTbl requestBody) {
        try {
            Integer affected = centralService.correctMachineUuid(requestBody);
            return ApiResult.getSuccessInstance(affected);
        } catch (Throwable e) {
            logger.info("[[tag=centralService]] correctUuidInMetaDb fail, requestBody: {}", requestBody, e);
            return ApiResult.getFailInstance(0, "correctUuidInMetaDb fail");
        }
    }

}
