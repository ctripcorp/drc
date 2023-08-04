package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName MonitorV2Controller
 * @Author haodongPan
 * @Date 2023/7/26 15:19
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/monitor/")
public class MonitorV2Controller {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired private MonitorServiceV2 monitorServiceV2;

    @GetMapping("mhaNames")
    public ApiResult queryMhaNamesToBeMonitored() {
        try {
            logger.info("[[monitor=mhaNames]] get all mhaNames");
            List<String> mhaNames = monitorServiceV2.getMhaNamesToBeMonitored();
            return ApiResult.getSuccessInstance(mhaNames);
        } catch (Exception e) {
            logger.error("[[monitor=mhaNames]] queryMhaNamesToBeMonitored fail",e);
            return ApiResult.getFailInstance(null,"queryMhaNamesToBeMonitored fail");
        }
    }
    
    
}
