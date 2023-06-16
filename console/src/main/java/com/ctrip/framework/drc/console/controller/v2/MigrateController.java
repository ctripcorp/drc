package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.service.v2.MetaMigrateService;
import com.ctrip.framework.drc.console.vo.response.migrate.MhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateMhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/6/16 17:58
 */
@RestController
@RequestMapping("/api/drc/v2/migrate")
public class MigrateController {

    @Autowired
    private MetaMigrateService metaMigrateService;

    @GetMapping ("/nameMapping")
    public ApiResult<List<String>> checkNameMapping() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.checkNameMapping());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @GetMapping ("/nameFilter")
    public ApiResult<MhaDbMappingResult> checkMhaFilter() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.checkMhaFilter());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/nameFilter")
    public ApiResult<Integer> splitNameFilter(@RequestBody List<NameFilterSplitParam> paramList) {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.splitNameFilter(paramList));
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/nameMapping")
    public ApiResult<Integer> splitNameFilterWithNameMapping() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.splitNameFilterWithNameMapping());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/region")
    public ApiResult<Integer> batchInsertRegions(@RequestBody List<String> regionNames) {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.batchInsertRegions(regionNames));
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/dc")
    public ApiResult<Integer> setDcRegions(@RequestBody Map<String, String> dcRegionMa) {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.batchUpdateDcRegions(dcRegionMa));
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/mha")
    public ApiResult<MigrateResult> migrateMhaTbl() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateMhaTbl());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/mhaReplication")
    public ApiResult<Integer> migrateMhaReplication() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateMhaReplication());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/applierGroup")
    public ApiResult<MigrateResult> migrateApplierGroup() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateApplierGroup());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/applier")
    public ApiResult<MigrateResult> migrateApplier() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateApplier());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @GetMapping ("/mhaDbMapping")
    public ApiResult<MhaDbMappingResult> checkMhaDbMapping() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.checkMhaDbMapping());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/mhaDbMapping")
    public ApiResult<MigrateMhaDbMappingResult> migrateMhaDbMapping() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateMhaDbMapping());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/columnsFilter")
    public ApiResult<MigrateResult> migrateColumnsFilter() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateColumnsFilter());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/dbReplication")
    public ApiResult<MigrateResult> migrateDbReplicationTbl() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateDbReplicationTbl());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/messengerGroup")
    public ApiResult<MigrateResult> migrateMessengerGroup() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateMessengerGroup());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/messengerFilter")
    public ApiResult<MigrateResult> migrateMessengerFilter() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateMessengerFilter());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }

    @PostMapping("/dbReplicationFilterMapping")
    public ApiResult<MigrateResult> migrateDbReplicationFilterMapping() {
        try {
            return ApiResult.getSuccessInstance(metaMigrateService.migrateDbReplicationFilterMapping());
        } catch (Exception e) {
            return ApiResult.getFailInstance(e.getMessage());
        }
    }
}
