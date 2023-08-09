package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by dengquanliang
 * 2023/8/8 14:54
 */
@RestController
@RequestMapping("/api/drc/v2/mha/")
public class MhaControllerV2 {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaServiceV2 mhaService;

    @PostMapping("tag")
    public ApiResult<Boolean> updateMhaTag(@RequestParam String mhaName, @RequestParam String tag) {
        try {
            mhaService.updateMhaTag(mhaName, tag);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
        return ApiResult.getSuccessInstance(true);
    }
}
