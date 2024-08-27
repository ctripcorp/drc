package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.service.v2.DbReplicationService;
import com.ctrip.framework.drc.console.vo.display.v2.DbReplicationVo;
import com.ctrip.framework.drc.console.vo.request.MqReplicationQueryDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by shiruixin
 * 2024/8/27 11:34
 */
@RestController("DbReplicationController")
@RequestMapping("/api/drc/v2/dbReplication/")
public class DbReplicationController {
    private static final Logger logger = LoggerFactory.getLogger(DbReplicationController.class);

    @Autowired
    DbReplicationService dbReplicationService;

    @GetMapping("mqReplication")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MQ_REPLICATION, attr = OperateAttrEnum.QUERY,
            success = "queryMqReplicationsByPage with MqReplicationQueryDto: {#queryDto.toString()}")
    public ApiResult<PageResult<DbReplicationVo>> queryMqReplicationsByPage(MqReplicationQueryDto queryDto) {
        logger.info("[meta] MqReplicationQueryDto :{}", queryDto.toString());
        try {
            return ApiResult.getSuccessInstance(dbReplicationService.queryMqReplicationsByPage(queryDto));
        }catch (Exception e) {
            logger.error("queryMqReplicationsByPage fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
