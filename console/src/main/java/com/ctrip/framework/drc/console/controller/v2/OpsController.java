package com.ctrip.framework.drc.console.controller.v2;

/**
 * @ClassName OpsController
 * @Author haodongPan
 * @Date 2024/5/10 14:08
 * @Version: $
 */

import com.ctrip.framework.drc.console.param.v2.GtidCompensateParam;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/drc/v2/ops")
public class OpsController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RowsFilterServiceV2 rowsFilterServiceV2;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private AccountService accountService;
    @Autowired
    private MhaServiceV2 mhaServiceV2;

    @GetMapping("migrate/sgp/rowsFilter")
    public ApiResult getRowsFilterIdsShouldMigrateToSGP(@RequestParam String srcRegion) {
        try {
            List<Long> rowsFilterIds = rowsFilterServiceV2.queryRowsFilterIdsShouldMigrate(srcRegion);
            return ApiResult.getSuccessInstance(rowsFilterIds);
        } catch (Exception e) {
            logger.error("getRowsFilterIdsShouldMigrateToSGP error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("migrate/sgp/rowsFilter")
    public ApiResult migrateRowsFilterToSGP(@RequestBody List<Long> rowsFilterIds,@RequestParam String srcRegion,@RequestParam String dstRegion) {
        try {
            Pair<Boolean, Integer> result = rowsFilterServiceV2.migrateRowsFilterUDLRegion(rowsFilterIds,srcRegion,dstRegion);
            return ApiResult.getSuccessInstance(result, "migrate rows filter to SGP success");
        } catch (Exception e) {
            logger.error("migrateRowsFilterToSGP error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("gtid/gapCompensate")
    public ApiResult gapCompensate(@RequestBody GtidCompensateParam gtidCompensateParam) {
        try {
            return  ApiResult.getSuccessInstance(drcBuildServiceV2.compensateGtidGap(gtidCompensateParam));
        } catch (Exception e) {
            logger.error("gapCompensate error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("account/passwordToken")
    public ApiResult initMhaPasswordToken(@RequestBody List<String> mhas) {
        try {
            Pair<Boolean, Integer> result = accountService.initMhaPasswordToken(mhas);
            return ApiResult.getSuccessInstance(result, "init mha password token success");
        } catch (Exception e) {
            logger.error("initMhaPasswordToken error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("account/accountV2")
    public ApiResult initMhaAccountV2(@RequestBody List<String> mhas,@RequestParam boolean forceChange) {
        try {
            return ApiResult.getSuccessInstance(accountService.mhaAccountV2ChangePwd(mhas, forceChange), "init mha account v2 success");
        } catch (Exception e) {
            logger.error("initMhaAccountV2 error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("account/accountV2Check")
    public ApiResult checkNewAccounts(@RequestBody List<String> mhas) {
        try {
            return ApiResult.getSuccessInstance(accountService.accountV2Check(mhas));
        } catch (Exception e) {
            logger.error("getMhaAccountV2 error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    

    @PostMapping("bu/isolation/mha")
    public ApiResult updateMhaTag(@RequestBody List<String> mhas, @RequestParam String tag) {
        try {
            logger.info("changeMhasTag mhas:{}, tag:{}", mhas, tag);
            return ApiResult.getSuccessInstance(mhaServiceV2.updateMhaTag(mhas, tag));
        } catch (Exception e) {
            logger.error("isolateMha error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    

    @PostMapping("bu/isolation/replicator")
    public ApiResult isolateReplicator(
            @RequestBody List<String> mhas, 
            @RequestParam boolean master, 
            @RequestParam String tag, 
            @RequestParam(required = false,defaultValue = "") String gtid //only for special case
    ) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.isolationMigrateReplicator(mhas, master, tag, gtid));
        } catch (Exception e) {
            logger.error("isolateReplicator error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("bu/isolation/applier")
    public ApiResult isolateApplier(@RequestBody List<String> mhas,@RequestParam String tag) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.isolationMigrateApplier(mhas, tag));
        } catch (Exception e) {
            logger.error("isolateApplier error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("bu/isolation/status")
    public ApiResult checkStatus(@RequestBody List<String> mhas,@RequestParam String tag) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.checkIsoMigrateStatus(mhas, tag));
        } catch (Exception e) {
            logger.error("isolateApplier error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    

}