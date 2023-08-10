package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.http.ApiResult;
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
 * 2023/8/9 20:29
 */
@RestController
@RequestMapping("/api/drc/v2/mha/")
public class MhaControllerV2 {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaServiceV2 mhaServiceV2;

    @GetMapping("replicator")
    public ApiResult<List<String>> getMhaReplicators(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaReplicators(mhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("resources")
    public ApiResult<List<String>> getMhaAvailableResource(@RequestParam String mhaName, @RequestParam int type) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaAvailableResource(mhaName, type));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("messenger")
    public ApiResult<List<String>> getMhaMessengers(@RequestParam(name = "mhaName") String mhaName) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaMessengers(mhaName));
        } catch (Throwable e) {
            logger.error("getMhaMessengers for {} exception ", mhaName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
