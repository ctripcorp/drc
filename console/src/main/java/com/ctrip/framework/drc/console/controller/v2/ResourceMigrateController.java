package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.v2.resource.DeleteIpParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceMigrateService;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/8 20:06
 */
@RestController
@RequestMapping("/api/drc/v2/resource/migrate")
public class ResourceMigrateController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ResourceMigrateService resourceMigrateService;

    @GetMapping("/unused/type")
    public ApiResult<List<ResourceView>> getResourceUnused(@RequestParam int type) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.getResourceUnused(type));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/deleted")
    public ApiResult<List<ResourceView>> getDeletedIps(@RequestBody DeleteIpParam param) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.getDeletedIps(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("/unused")
    public ApiResult<Integer> deleteResourceUnused(@RequestBody List<String> ips) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.deleteResourceUnused(ips));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("")
    public ApiResult<Integer> updateResource(@RequestBody List<ResourceBuildParam> params) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.updateResource(params));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/mha")
    public ApiResult<Integer> updateMhaTag() {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.updateMhaTag());
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/replicator")
    public ApiResult<List<Long>> getReplicatorGroupIdsWithSameAz() {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.getReplicatorGroupIdsWithSameAz());
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/applier")
    public ApiResult<List<Long>> getApplierGroupIdsWithSameAz() {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.getApplierGroupIdsWithSameAz());
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/messenger")
    public ApiResult<List<Long>> getMessengerGroupIdsWithSameAz() {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.getMessengerGroupIdsWithSameAz());
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
