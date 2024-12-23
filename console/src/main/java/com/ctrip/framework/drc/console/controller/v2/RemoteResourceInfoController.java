package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.RemoteResourceService;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author yongnian
 * @create 2024/12/18 12:53
 */
@RequestMapping("/api/drc/v2/remote/resource")
@RestController
public class RemoteResourceInfoController {

    private static final Logger log = LoggerFactory.getLogger(RemoteResourceInfoController.class);
    @Autowired
    private RemoteResourceService remoteResourceService;

    @GetMapping("dbApplierInstances")
    @SuppressWarnings("unchecked")
    public ApiResult<Map<String, List<Instance>>> dbApplierInstances(@RequestParam("src") String srcMha, @RequestParam("mhaName") String dstMha,
                                                                      @RequestParam("ips") List<String> ips) {
        try {
            Map<String, List<Instance>> currentReplicatorInstance = remoteResourceService.getCurrentDbApplierInstances(srcMha, dstMha, ips);
            return ApiResult.getSuccessInstance(currentReplicatorInstance);
        } catch (Throwable e) {
            log.error("dbApplierInstances, src:{}, dst: {} ips {}", srcMha, dstMha, ips, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("replicatorInstances")
    @SuppressWarnings("unchecked")
    public ApiResult<List<Instance>> replicatorInstances(@RequestParam("mhaName") String mhaName,
                                                         @RequestParam("ips") List<String> ips) {
        try {
            List<Instance> currentReplicatorInstance = remoteResourceService.getCurrentReplicatorInstance(mhaName, ips);
            return ApiResult.getSuccessInstance(currentReplicatorInstance);
        } catch (Throwable e) {
            log.error("replicatorInstances, mhaName {} ips {}", mhaName, ips, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("messengerInstances")
    @SuppressWarnings("unchecked")
    public ApiResult<List<Instance>> messengerInstances(@RequestParam("mhaName") String mhaName,
                                                              @RequestParam("ips") List<String> ips) {
        try {
            List<Instance> currentMessengerInstance = remoteResourceService.getCurrentMessengerInstance(mhaName, ips);
            return ApiResult.getSuccessInstance(currentMessengerInstance);
        } catch (Throwable e) {
            log.error("messengerInstances, mhaName {} ips {}", mhaName, ips, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
