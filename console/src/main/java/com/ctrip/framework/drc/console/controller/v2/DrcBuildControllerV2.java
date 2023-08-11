package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.DrcConfigView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/9 16:58
 */
@RestController
@RequestMapping("/api/drc/v2/config/")
public class DrcBuildControllerV2 {

    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;

    @PostMapping("mha")
    public ApiResult<Boolean> buildMha(@RequestBody DrcMhaBuildParam param) {
        try {
            drcBuildServiceV2.buildMha(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("")
    public String buildDrc(@RequestBody DrcBuildParam param) {
        try {
            drcBuildServiceV2.buildDrc(param);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("")
    public ApiResult<DrcConfigView> getDrcConfig(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getDrcConfigView(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/applier")
    public ApiResult<List<String>> getMhaAppliers(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getMhaAppliers(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/applierGtid")
    public ApiResult<String> getApplierGtid(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getApplierGtid(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbReplication")
    public ApiResult<List<Long>> configureDbReplications(@RequestBody DbReplicationBuildParam param) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.configureDbReplications(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("dbReplication")
    public ApiResult<List<DbReplicationView>> getDbReplicationView(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getDbReplicationView(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("dbReplication")
    public ApiResult<Boolean> deleteDbReplications(@RequestParam long dbReplicationId) {
        try {
            drcBuildServiceV2.deleteDbReplications(dbReplicationId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("columnsFilter")
    public ApiResult<Boolean> buildColumnsFilter(@RequestBody ColumnsFilterCreateParam param) {
        try {
            drcBuildServiceV2.buildColumnsFilter(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("columnsFilter")
    public ApiResult<ColumnsConfigView> getColumnsConfigView(@RequestParam long dbReplicationId) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getColumnsConfigView(dbReplicationId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping("columnsFilter")
    public ApiResult<Boolean> deleteColumnsFilter(@RequestBody List<Long> dbReplicationIds) {
        try {
            drcBuildServiceV2.deleteColumnsFilter(dbReplicationIds);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("rowsFilter")
    public ApiResult<RowsFilterConfigView> getRowsConfigView(@RequestParam long dbReplicationId) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getRowsConfigView(dbReplicationId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("rowsFilter")
    public ApiResult<Boolean> buildRowsFilter(@RequestBody RowsFilterCreateParam param) {
        try {
            drcBuildServiceV2.buildRowsFilter(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping("rowsFilter")
    public ApiResult<Boolean> deleteRowsFilter(@RequestBody List<Long> dbReplicationIds) {
        try {
            drcBuildServiceV2.deleteRowsFilter(dbReplicationIds);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
}
