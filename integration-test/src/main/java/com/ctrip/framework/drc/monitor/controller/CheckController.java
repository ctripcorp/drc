package com.ctrip.framework.drc.monitor.controller;

import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by jixinwang on 2022/11/23
 */
@RestController
@RequestMapping("/api")
public class CheckController {

    @GetMapping("/health")
    public ApiResult health() {
        return ApiResult.getSuccessInstance("success");
    }
}
