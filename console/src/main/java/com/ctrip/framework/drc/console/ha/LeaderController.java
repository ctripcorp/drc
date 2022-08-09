package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @ClassName LeaderController
 * @Author haodongPan
 * @Date 2022/6/27 14:53
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/leader/")
public class LeaderController {
    
    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;
    
    @GetMapping ("status")
    public ApiResult status(){
        return ApiResult.getSuccessInstance(consoleLeaderElector.amILeader() ? "leader" : "not leader");
    }

    
}
