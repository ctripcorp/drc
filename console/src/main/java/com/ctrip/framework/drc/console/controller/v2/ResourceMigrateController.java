package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceMigrateService;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import qunar.api.open.annotation.RequestParam;

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

    @DeleteMapping("/unused")
    public ApiResult<Integer> deleteResourceUnused(@RequestBody List<String> ips) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.deleteResourceUnused(ips));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("/unused/type")
    public ApiResult<Integer> deleteResourceUnused(@RequestParam int type) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.deleteResourceUnused(type));
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

    @PostMapping("/dc")
    public ApiResult<Integer> updateResource(@RequestParam String dc) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.updateResource(dc));
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

    @PostMapping("/replicator/offline")
    public ApiResult<Integer> offlineReplicatorWithSameAz(@RequestBody List<Long> replicatorGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.offlineReplicatorWithSameAz(replicatorGroupIds));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/replicator/online")
    public ApiResult<Integer> onlineReplicatorWithSameAz(@RequestBody List<Long> replicatorGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.onlineReplicatorWithSameAz(replicatorGroupIds));
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

    @PostMapping("/applier/offline")
    public ApiResult<Integer> offlineApplierWithSameAz(@RequestBody List<Long> applierGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.offlineApplierWithSameAz(applierGroupIds));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/applier/online")
    public ApiResult<Integer> onlineApplierWithSameAz(@RequestBody List<Long> applierGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.onlineApplierWithSameAz(applierGroupIds));
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

    @PostMapping("/messenger/offline")
    public ApiResult<Integer> offlineMessengerWithSameAz(@RequestBody List<Long> messengerGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.offlineMessengerWithSameAz(messengerGroupIds));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/messenger/online")
    public ApiResult<Integer> onlineMessengerWithSameAz(@RequestBody List<Long> messengerGroupIds) {
        try {
            return ApiResult.getSuccessInstance(resourceMigrateService.onlineMessengerWithSameAz(messengerGroupIds));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
