package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wangjixin
 * @version 1.0
 * date: 2020-03-03
 */


@RestController
@RequestMapping("/api/drc/v1")
public class UserController {

    
    private UserService userService = ApiContainer.getUserServiceImpl();

    @GetMapping(value = "/user/current")
    public ApiResult getInfo() {
        return ApiResult.getSuccessInstance(userService.getInfo());
    }

    @GetMapping(value = "/user/logout")
    public ApiResult getLogoutUrl() {
        return ApiResult.getSuccessInstance(userService.getLogoutUrl());
    }
}
