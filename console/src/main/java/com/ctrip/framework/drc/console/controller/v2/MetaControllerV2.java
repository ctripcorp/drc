package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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
    private MetaGeneratorV2 metaGenerator;

    @GetMapping
    public ApiResult<String> getAllMetaData() {
        logger.info("[meta] get all");
        try {
            Drc drc = metaGenerator.getDrc();
            logger.info("drc:\n {}", drc.toString());
            return ApiResult.getSuccessInstance(drc.toString());
        } catch (Exception e) {
            logger.error("get drc fail: {}", e);
            return ApiResult.getFailInstance(String.format("get drc fail, %s", e));
        }
    }
}
