package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.v2.resource.*;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaDbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.MhaReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
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
            return ApiResult.getSuccessInstance(resourceService.getMhaAvailableResourceWithUse(mhaName, type));
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

    @GetMapping("db/all")
    public ApiResult<List<ResourceView>> getMhaDbAvailableResource(DbResourceSelectParam param) {
        try {
            return ApiResult.getSuccessInstance(resourceService.getMhaDbAvailableResourceWithUse(param.getSrcMhaName(), param.getDstMhaName(), param.getType()));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    @GetMapping("db/auto")
    public ApiResult<List<ResourceView>> autoConfigureMhaDbResource(DbResourceSelectParam param) {
        try {
            return ApiResult.getSuccessInstance(resourceService.autoConfigureMhaDbResource(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PutMapping
    @LogRecord(type = OperateTypeEnum.DRC_RESOURCE,attr = OperateAttrEnum.ADD,
    success = "configureResource with ResourceBuildParam {#param}")
    public ApiResult<Boolean> configureResource(@RequestBody ResourceBuildParam param) {
        try {
            resourceService.configureResource(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping
    @LogRecord(type = OperateTypeEnum.DRC_RESOURCE,attr = OperateAttrEnum.DELETE,
    success = "offlineResource with resourceId {#resourceId}")
    public ApiResult<Boolean> offlineResource(@RequestParam long resourceId) {
        try {
            resourceService.offlineResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("deactivate")
    @LogRecord(type = OperateTypeEnum.DRC_RESOURCE,attr = OperateAttrEnum.UPDATE,
    success = "deactivateResource with resourceId {#resourceId}")
    public ApiResult<Boolean> deactivateResource(@RequestParam long resourceId) {
        try {
            resourceService.deactivateResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("active")
    @LogRecord(type = OperateTypeEnum.DRC_RESOURCE,attr = OperateAttrEnum.UPDATE,
    success = "recoverResource with resourceId {#resourceId}")
    public ApiResult<Boolean> recoverResource(@RequestParam long resourceId) {
        try {
            resourceService.recoverResource(resourceId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("mha")
    public ApiResult<List<String>> queryMhaByReplicator(@RequestParam long resourceId) {
        try {
            return ApiResult.getSuccessInstance(resourceService.queryMhaByReplicator(resourceId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("mha/messenger")
    public ApiResult<List<String>> queryMhaByMessenger(@RequestParam long resourceId) {
        try {
            return ApiResult.getSuccessInstance(resourceService.queryMhaByMessenger(resourceId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("mhaReplication")
    public ApiResult<List<MhaReplicationView>> queryMhaReplicationByApplier(@RequestParam long resourceId) {
        try {
            return ApiResult.getSuccessInstance(resourceService.queryMhaReplicationByApplier(resourceId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("mhaDbReplication")
    public ApiResult<List<MhaDbReplicationView>> queryMhaDbReplicationByApplier(@RequestParam long resourceId) {
        try {
            return ApiResult.getSuccessInstance(resourceService.queryMhaDbReplicationByApplier(resourceId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("migrate/replicator")
    public ApiResult<Integer> migrateReplicator(@RequestParam String newIp, @RequestParam String oldIp) {
        try {
            return ApiResult.getSuccessInstance(resourceService.migrateResource(newIp, oldIp, ModuleEnum.REPLICATOR.getCode()));
        } catch (Exception e) {
            logger.error("migrateReplicator fail, ", e);
            return ApiResult.getFailInstance(0, e.getMessage());
        }
    }

    @PostMapping("migrate/applier")
    public ApiResult<Integer> migrateApplier(@RequestParam String newIp, @RequestParam String oldIp) {
        try {
            return ApiResult.getSuccessInstance(resourceService.migrateResource(newIp, oldIp, ModuleEnum.APPLIER.getCode()));
        } catch (Exception e) {
            logger.error("migrateApplier fail, ", e);
            return ApiResult.getFailInstance(0, e.getMessage());
        }
    }

    @PostMapping("migrate/resource")
    public ApiResult<Boolean> migrateResource(@RequestBody ResourceMigrateParam param) {
        try {
            resourceService.migrateResource(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("migrateResource fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
}
