package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.MetaServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    private MetaServiceV2 metaServiceV2;

    @GetMapping
    public String getAllMetaData() {
        logger.info("[meta] get all");
        try {
            Drc drc = metaProviderV2.getDrc();
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

    @GetMapping("compareRes/{dbclusterId}")
    public ApiResult<DbClusterCompareRes> compareOldNewMeta(@PathVariable String dbclusterId) {
        logger.info("[[tag=metaCompare]] start compareOldNewMeta,dbclusterId:{}",dbclusterId);
        try {
            return ApiResult.getSuccessInstance(metaServiceV2.compareDbCluster(dbclusterId));
        } catch (Throwable e) {
            logger.error("[[tag=metaCompare]] compareOldNewMeta error",e);
            return ApiResult.getFailInstance("compareOldNewMeta error");
        }
    }
}
