package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;


/**
 * @ClassName TestController
 * @Author haodongPan
 * @Date 2022/6/27 14:53
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/test/")
public class TestController {
    private static final Logger logger = LoggerFactory.getLogger(TestController.class);
    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;
    
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    
    @PostMapping ("register")
    public ApiResult register(){
        try {
            consoleLeaderElector.initialize();
            consoleLeaderElector.start();
            return ApiResult.getSuccessInstance("start to elect");
        } catch (Exception e) {
            logger.error("leader elect error",e);
            return ApiResult.getFailInstance("error in leader elector");
        } 
    }


    @PostMapping ("deRegister")
    public ApiResult deRegister(){
        try {
            consoleLeaderElector.stop();
            return ApiResult.getSuccessInstance("close elect");
        } catch (Exception e) {
            logger.error("close elect error",e);
            return ApiResult.getFailInstance("error in close elect");
        }
    }
    
    @GetMapping ("status")
    public ApiResult status(){
        return ApiResult.getSuccessInstance(consoleLeaderElector.amILeader() ? "leader" : "not leader");
    }

    @GetMapping ("properties")
    public ApiResult properties(){
        Map<String, List<String>> regionsInfo = defaultConsoleConfig.getRegionsInfo();
        return ApiResult.getSuccessInstance(regionsInfo);
    }
}
