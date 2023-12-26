package com.ctrip.framework.drc.console.controller.v1;

import com.ctrip.framework.drc.console.aop.AuthToken;
import com.ctrip.framework.drc.console.aop.RateLimit;
import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.enums.HttpRequestParamEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaService;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMessageVO;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @RateLimit
    @AuthToken(name = "metaFilterName", type = HttpRequestParamEnum.PATH_VARIABLE)
    @GetMapping("/metaFilterName/{metaFilterName}")
    public ApiResult<QConfigDataVO> getWhitelist(@PathVariable String metaFilterName) {
        try {
            logger.info("Get Rows Filter Whitelist, metaFilterName: {}", metaFilterName);
            return ApiResult.getSuccessInstance(rowsFilterMetaService.getWhitelist(metaFilterName));
        } catch (Exception e) {
            logger.error("Get Rows Filter Whitelist Error, metaFilterName: {}", metaFilterName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @RateLimit
    @AuthToken(name = "metaFilterName", type = HttpRequestParamEnum.REQUEST_BODY)
    @PutMapping()
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.ADD,
            success = "addWhitelist with  operator:{#operator},RowsMetaFilterParam: {#param.toString()}",operator = "USER")
    public ApiResult<Boolean> addWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Add Rows Filter Whitelist, param: {}", param);
            boolean result = rowsFilterMetaService.addWhitelist(param, operator);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Add Config Fail");
            }
        } catch (Exception e) {
            logger.error("Add Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @RateLimit
    @AuthToken(name = "metaFilterName", type = HttpRequestParamEnum.REQUEST_BODY)
    @DeleteMapping()
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.DELETE,
            success = "deleteWhitelist with operator:{#operator},RowsMetaFilterParam: {#param.toString()}",operator = "USER")
    public ApiResult<Boolean> deleteWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Delete Rows Filter Whitelist, param: {}", param);
            boolean result = rowsFilterMetaService.deleteWhitelist(param, operator);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Delete Config Fail");
            }
        } catch (Exception e) {
            logger.error("Delete Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @RateLimit
    @AuthToken(name = "metaFilterName", type = HttpRequestParamEnum.REQUEST_BODY)
    @PostMapping()
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.UPDATE,
            success = "updateWhitelist with operator:{#operator},RowsMetaFilterParam: {#param.toString()}",operator = "USER")
    public ApiResult<Boolean> updateWhitelist(@RequestBody RowsMetaFilterParam param, @RequestParam String operator) {
        try {
            logger.info("Update Rows Filter Whitelist, param: {}", param);
            boolean result = rowsFilterMetaService.updateWhitelist(param, operator);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Update Config Fail");
            }
        } catch (Exception e) {
            logger.error("Update Rows Filter Whitelist Error, param: {}", param, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PutMapping("/meta")
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.ADD,
            success = "createMetaMessage with RowsFilterMetaMessageCreateParam:{#param.toString()}")
    public ApiResult<Boolean> createMetaMessage(@RequestBody RowsFilterMetaMessageCreateParam param) {
        try {
            logger.info("Create Meta Message, param: {}", param);
            boolean result = rowsFilterMetaMappingService.createMetaMessage(param);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Create MetaMessage Fail");
            }
        } catch (Exception e) {
            logger.error("Create Meta Message Error, param: {}", param, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("/mapping")
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.ADD,
            success = "createOrUpdateMetaMapping with RowsFilterMetaMappingCreateParam:{#param.toString()}")
    public ApiResult<Boolean> createOrUpdateMetaMapping(@RequestBody RowsFilterMetaMappingCreateParam param) {
        try {
            logger.info("Create Meta Mapping, param: {}", param);
            boolean result = rowsFilterMetaMappingService.createOrUpdateMetaMapping(param);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Update MetaMapping Fail");
            }
        } catch (Exception e) {
            logger.error("Create Meta Mapping Error, param: {}", param, e);
            return ApiResult.getFailInstance(false,e.getMessage());
        }
    }

    @GetMapping("/meta/all")
    public ApiResult<List<RowsFilterMetaMessageVO>> getMetaMessageList(@RequestParam(required = false) String metaFilterName, @RequestParam(required = false) String mhaName) {
        try {
            logger.info("Get Rows Filter Meta Message List, metaFilterName: {}, mhaName: {}", metaFilterName, mhaName);
            return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.getMetaMessages(metaFilterName, mhaName));
        } catch (Exception e) {
            logger.error("Get Rows Filter Meta Message List error, metaFilterName: {}", metaFilterName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/mapping")
    public ApiResult<RowsFilterMetaMappingVO> getMappingList(@RequestParam Long metaFilterId) {
        try {
            logger.info("Get Rows Filter Meta Mapping List, metaFilterId: {}", metaFilterId);
            return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.getMetaMappings(metaFilterId));
        } catch (Exception e) {
            logger.error("Get Rows Filter Meta Mapping List error, metaFilterId: {}", metaFilterId, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping ("/meta")
    @LogRecord(type = OperateTypeEnum.ROWS_FILTER_MARK, attr = OperateAttrEnum.DELETE,
            success = "deleteMetaMessage with metaFilterId:{#metaFilterId}")
    public ApiResult<Boolean> deleteMetaMessage(@RequestParam Long metaFilterId) {
        try {
            logger.info("Delete Rows Filter Meta Message, metaFilterId: {}", metaFilterId);
            boolean result = rowsFilterMetaMappingService.deleteMetaMessage(metaFilterId);
            if (result) {
                return ApiResult.getSuccessInstance(true);
            } else {
                return ApiResult.getFailInstance(false, "Delete MetaMessage Fail");
            }
        } catch (Exception e) {
            logger.error("Delete Rows Filter Meta Message error, metaFilterId: {}", metaFilterId, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("/subEnv")
    public ApiResult<List<String>> getTargetSubEnvs() {
        return ApiResult.getSuccessInstance(rowsFilterMetaMappingService.getTargetSubEnvs());
    }

}
