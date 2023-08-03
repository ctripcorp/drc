package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaGrayService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/2 14:47
 */
@RestController
@RequestMapping("/api/drc/v2/meta/")
public class MetaControllerV2 {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaProviderV2 metaProviderV2;

    @Autowired
    private MetaGrayService metaServiceV2;

    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Autowired
    private MetaGeneratorV2 metaGeneratorV2;

    @GetMapping
    public String getAllMetaData(@RequestParam(value = "refresh" , required = false, defaultValue = "false") String refresh) {
        logger.info("[meta] get all");
        try {
            Drc drc;
            if (StringUtils.equals("true",refresh)) {
                metaProviderV2.scheduledTask();
                drc = metaProviderV2.getDrc();
            } else {
                drc = metaProviderV2.getDrc();
            }
            logger.info("drc:\n {}", drc.toString());
            return drc.toString();
        } catch (Exception e) {
            logger.error("get drc fail: {}", e);
            return "get drc fail";
        }
    }

    @GetMapping("compareRes")
    public ApiResult<String> compareOldNewMeta() {
        logger.info("[[tag=metaCompare]] start compareOldNewMeta");
        try {
            String compareRecorder = metaServiceV2.compareDrcMeta();
            if (compareRecorder.contains("not equal") || compareRecorder.contains("empty") || compareRecorder.contains("fail")) {
                return ApiResult.getSuccessInstance(compareRecorder,"not equal");
            } else {
                return ApiResult.getSuccessInstance(compareRecorder,"equal");
            }
        } catch (Throwable e) {
            logger.error("[[tag=metaCompare]] compareOldNewMeta error");
            return ApiResult.getFailInstance(e,"compareOldNewMeta error");
        }
    }

    @GetMapping("compareRes/dbcluster")
    public ApiResult<DbClusterCompareRes> compareOldNewMeta(@RequestParam String dbclusterId) {
        logger.info("[[tag=metaCompare]] start compareOldNewMeta,dbclusterId:{}",dbclusterId);
        try {
            DbClusterCompareRes res = metaServiceV2.compareDbCluster(dbclusterId);
            String compareRes = res.getCompareRes();
            if (compareRes.contains("not equal") || compareRes.contains("empty") || compareRes.contains("fail")) {
                return ApiResult.getSuccessInstance(res,"not equal");
            } else {
                return ApiResult.getSuccessInstance(res,"equal");
            }
        } catch (Throwable e) {
            logger.error("[[tag=metaCompare]] compareOldNewMeta error,dbclusterId:{}",dbclusterId,e);
            return ApiResult.getFailInstance("compareOldNewMeta error");
        }
    }

    @GetMapping("bus/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<BuTbl>> getAllBuTbls() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllBuWithCache());
        } catch (ConsoleException e) {
            logger.error("[meta] getAllBuTbls exception" + e.getMessage());
            return ApiResult.getFailInstance(e.getMessage());
        } catch (Throwable e) {
            logger.error("[meta] getAllBuTbls error", e);
            return ApiResult.getFailInstance("unknown exception:" + e.getMessage());
        }
    }

    @GetMapping("regions/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RegionTbl>> getAllRegionTbls() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllRegionWithCache());
        } catch (ConsoleException e) {
            logger.error("[meta] getAllBuTbls exception" + e.getMessage());
            return ApiResult.getFailInstance(e.getMessage());
        } catch (Throwable e) {
            logger.error("[meta] getAllBuTbls error", e);
            return ApiResult.getFailInstance("unknown exception:" + e.getMessage());
        }
    }

    @GetMapping("dcs/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DcDo>> getAllDcs() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllDcWithCache());
        } catch (ConsoleException e) {
            logger.error("[meta] getAllDcs exception" + e.getMessage());
            return ApiResult.getFailInstance(e.getMessage());
        } catch (Throwable e) {
            logger.error("[meta] getAllDcs error", e);
            return ApiResult.getFailInstance("unknown exception:" + e.getMessage());
        }
    }

}
