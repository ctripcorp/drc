package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildTaskService;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.display.v2.MhaVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/8/9 16:58
 */
@RestController
@RequestMapping("/api/drc/v2/autoconfig/")
public class DbDrcBuildControllerV2 {
    private static final Logger logger = LoggerFactory.getLogger(DbDrcBuildControllerV2.class);

    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;

    @Autowired
    private DrcAutoBuildTaskService drcAutoBuildTaskService;

    @Autowired
    private DbaApiService dbaApiService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private MetaInfoServiceV2 metaInfoService;


    @GetMapping("getDbClusterInfo")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaVo>> getDbClusterInfo(@RequestParam("dbName") String dbName) {
        logger.info("[meta] getClusterInfo, dbName: {}", dbName);
        try {
            List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(dbName);
            Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
            List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
            Map<String, DcDo> dcMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcName, e -> e));

            List<MhaVo> mhaVos = clusterInfoDtoList.stream().map(e -> {
                String dcName = dbaDc2DrcDcMap.get(e.getZoneId().toLowerCase());
                DcDo dcDo = dcMap.get(dcName);

                MhaVo vo = new MhaVo();
                vo.setName(e.getClusterName());
                vo.setDcId(dcDo.getDcId());
                vo.setDcName(dcDo.getDcName());
                vo.setRegionName(dcDo.getRegionName());
                return vo;
            }).collect(Collectors.toList());
            return ApiResult.getSuccessInstance(mhaVos);
        } catch (Throwable e) {
            logger.error("[meta] getClusterInfo, dbName {}", dbName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("getExistDb")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DbTbl>> getExistDb(@RequestParam("dbName") String dbName) {
        try {
            DbQuery query = new DbQuery();
            query.setLikeByDbNameFromBeginning(dbName);
            query.setPageIndex(1);
            query.setPageSize(20);
            List<DbTbl> dbTbls = metaInfoService.queryDbByPage(query);
            return ApiResult.getSuccessInstance(dbTbls);
        } catch (Throwable e) {
            logger.error("[meta] getClusterInfo, dbName {}", dbName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("submit")
    @SuppressWarnings("unchecked")
    public ApiResult<Void> autoBuildAll(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] auto build drc, req: {}", req);
        try {
            drcAutoBuildTaskService.autoBuildDrc(req);
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("[meta] auto build drc, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("autoBuildDrcOptions")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaReplicationPreviewDto>> getAllDbClusterInfo(@RequestParam("dalClusterName") String dalClusterName,
                                                                         @RequestParam("srcRegionName") String srcRegionName,
                                                                         @RequestParam("dstRegionName") String dstRegionName) {
        logger.info("[meta] getClusterInfo, dalClusterName: {}", dalClusterName);
        try {
            if (StringUtils.isEmpty(dalClusterName)) {
                return ApiResult.getFailInstance(null, "dalClusterName is empty");
            }
            List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
            Set<String> regionSet = dcDos.stream().map(DcDo::getRegionName).collect(Collectors.toSet());
            if (!regionSet.contains(srcRegionName)) {
                throw new IllegalArgumentException("srcRegionName is not in dcDos: " + srcRegionName);
            }
            if (!regionSet.contains(dstRegionName)) {
                throw new IllegalArgumentException("dstRegionName is not in dcDos: " + dstRegionName);
            }
            List<MhaReplicationPreviewDto> list = drcAutoBuildTaskService.previewAutoBuildOptions(dalClusterName, srcRegionName, dstRegionName);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] getClusterInfo, dbName {}", dalClusterName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
