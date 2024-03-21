package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dto.v2.MhaDbDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.vo.display.v2.DelayInfoVo;
import com.ctrip.framework.drc.console.vo.request.MhaDbReplicationQueryDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/api/drc/v2/replication/db/")
public class MhaDbReplicationController {
    private static final Logger logger = LoggerFactory.getLogger(MhaDbReplicationController.class);

    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;


    @GetMapping("query")
    @SuppressWarnings("unchecked")
    public ApiResult<PageResult<MhaDbReplicationDto>> queryByPage(MhaDbReplicationQueryDto queryDto) {
        logger.info("[meta] get allOrderedGroup,drcGroupQueryDto:{}", queryDto.toString());
        try {
            // query replication
            PageResult<MhaDbReplicationDto> tblPageResult = mhaDbReplicationService.query(queryDto);
            return ApiResult.getSuccessInstance(tblPageResult);
        } catch (Exception e) {
            logger.error("queryMhaReplicationsByPage error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    @GetMapping("delay")
    @SuppressWarnings("unchecked")
    public ApiResult<DelayInfoVo> getMhaReplicationDelay(@RequestParam(name = "replicationIds") List<Long> replicationIds) {
        try {
            if (CollectionUtils.isEmpty(replicationIds)) {
                return ApiResult.getSuccessInstance(Collections.emptyList());
            }

            List<MhaDbDelayInfoDto> mhaReplicationDelays = mhaDbReplicationService.getReplicationDelays(replicationIds);
            List<DelayInfoVo> res = mhaReplicationDelays.stream().map(DelayInfoVo::from).collect(Collectors.toList());
            return ApiResult.getSuccessInstance(res);
        } catch (Throwable e) {
            logger.error(String.format("getMhaReplicationDelay error: %s", replicationIds), e);
            return ApiResult.getFailInstance(e, e.getMessage());
        }
    }
}
