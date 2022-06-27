package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



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
            return ApiResult.getSuccessInstance("start to elect");
        } catch (Exception e) {
            logger.error("leader elect error",e);
            return ApiResult.getFailInstance("error in leader elector");
        }
    }
    
    @GetMapping ("status")
    public ApiResult status(){
        boolean b = consoleLeaderElector.amILeader();
        return ApiResult.getSuccessInstance(b);
    }
}
