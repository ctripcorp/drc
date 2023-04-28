package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaService;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 19:15
 */
@RestController
@RequestMapping("/api/drc/v1/filter/row")
public class RowsFilterMetaController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RowsFilterMetaMappingService rowsFilterMetaMappingService;
    @Autowired
    private RowsFilterMetaService rowsFilterMetaService;

    @GetMapping("/metaFilterName/{metaFilterName}")
    public ApiResult<QConfigDataVO> getWhitelist(@PathVariable String metaFilterName) {
        try {
            logger.info("Get Rows Filter Whitelist, metaFilterName: {}", metaFilterName);
            return ApiResult.getSuccessInstance(rowsFilterMetaService.getWhiteList(metaFilterName));
        } catch (SQLException e) {
            logger.error("Get Rows Filter Whitelist Error, metaFilterName: {}", metaFilterName, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @PutMapping()
    public ApiResult<Boolean> addWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Add Rows Filter Whitelist, param: {}", param);
            return ApiResult.getSuccessInstance(rowsFilterMetaService.addWhiteList(param, operator));
        } catch (SQLException e) {
            logger.error("Add Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @DeleteMapping()
    public ApiResult<Boolean> deleteWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Delete Rows Filter Whitelist, param: {}", param);
            return ApiResult.getSuccessInstance(rowsFilterMetaService.deleteWhiteList(param, operator));
        } catch (SQLException e) {
            logger.error("Delete Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @PostMapping()
    public ApiResult<Boolean> updateWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Update Rows Filter Whitelist, param: {}", param);
            return ApiResult.getSuccessInstance(rowsFilterMetaService.updateWhiteList(param, operator));
        } catch (SQLException e) {
            logger.error("Update Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @PutMapping("/meta")
    public ApiResult<Boolean> createMetaMessage(@RequestBody RowsFilterMetaMessageCreateParam param) {
        try {
            logger.info("Create Meta Message, param: {}", param);
            return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.createMetaMessage(param));
        } catch (SQLException e) {
            logger.error("Create Meta Message Error, param: {}", param, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @PutMapping("/mapping")
    public ApiResult<Boolean> createMetaMapping(@RequestBody RowsFilterMetaMappingCreateParam param) {
        try {
            logger.info("Create Meta Mapping, param: {}", param);
            return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.createMetaMapping(param));
        } catch (SQLException e) {
            logger.error("Create Meta Mapping Error, param: {}", param, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @GetMapping("/mapping/metaFilterName/{metaFilterName}")
    public ApiResult<List<RowsFilterMetaMappingVO>> getMappingList(@PathVariable String metaFilterName) {
        try {
            logger.info("Get Rows Filter Meta Mapping List, metaFilterName: {}", metaFilterName);
            return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.getMetaMappings(metaFilterName));
        } catch (SQLException e) {
            logger.error("Get Rows Filter Meta Mapping List error, metaFilterName: {}", metaFilterName, e);
            return ApiResult.getFailInstance(false);
        }
    }

}
