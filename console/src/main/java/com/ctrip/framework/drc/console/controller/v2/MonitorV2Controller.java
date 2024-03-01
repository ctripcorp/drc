package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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

    @GetMapping("dstMhaNames")
    public ApiResult queryDstMhaNamesToBeMonitored() {
        try {
            logger.info("[[monitor=mhaNames]] queryDstMhaNamesToBeMonitored");
            List<String> mhaNames = monitorServiceV2.getDestMhaNamesToBeMonitored();
            return ApiResult.getSuccessInstance(mhaNames);
        } catch (Exception e) {
            logger.error("[[monitor=mhaNames]] queryDstMhaNamesToBeMonitored fail",e);
            return ApiResult.getFailInstance(null,"queryDstMhaNamesToBeMonitored fail");
        }
    }

    @PostMapping("switch/{mhaName}/{status}")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> switchMonitors(@PathVariable String mhaName, @PathVariable String status) {
        try {
            monitorServiceV2.switchMonitors(mhaName, status);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

}
