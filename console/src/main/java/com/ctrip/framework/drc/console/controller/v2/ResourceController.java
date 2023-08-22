package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/8 15:00
 */
@RestController
@RequestMapping("/api/drc/v2/resource/")
public class ResourceController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceService resourceService;

    @GetMapping("all")
    public ApiResult<List<ResourceView>> getResourceView(ResourceQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(resourceService.getResourceView(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/all")
    public ApiResult<List<ResourceView>> getMhaAvailableResource(@RequestParam String mhaName, @RequestParam int type) {
        try {
            return ApiResult.getSuccessInstance(resourceService.getMhaAvailableResource(mhaName, type));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/auto")
    public ApiResult<List<ResourceView>> autoConfigureResource(ResourceSelectParam param) {
        try {
            return ApiResult.getSuccessInstance(resourceService.autoConfigureResource(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PutMapping
    public ApiResult<Boolean> configureResource(@RequestBody ResourceBuildParam param) {
        try {
            resourceService.configureResource(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping
    public ApiResult<Boolean> offlineResource(@RequestParam long resourceId) {
        try {
            resourceService.offlineResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("deactivate")
    public ApiResult<Boolean> deactivateResource(@RequestParam long resourceId) {
        try {
            resourceService.deactivateResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("active")
    public ApiResult<Boolean> recoverResource(@RequestParam long resourceId) {
        try {
            resourceService.recoverResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
}
