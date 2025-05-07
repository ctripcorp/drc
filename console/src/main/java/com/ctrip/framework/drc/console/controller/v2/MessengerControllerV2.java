package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MessengerDelayQueryDto;
import com.ctrip.framework.drc.console.vo.request.MessengerQueryDto;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.mq.MqType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController("MessengerControllerV2")
@RequestMapping("/api/drc/v2/messenger/")
public class MessengerControllerV2 {

    private static final Logger logger = LoggerFactory.getLogger(MessengerControllerV2.class);

    @Autowired
    MessengerServiceV2 messengerService;
    @Autowired
    MetaInfoServiceV2 metaInfoServiceV2;

    @GetMapping("query")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MessengerVo>> queryMessengerVos(MessengerQueryDto queryDto) {
        logger.info("[meta] MessengerQueryDto :{}", queryDto.toString());
        try {
            queryDto.validate();
            List<MessengerVo> messengerVoList = messengerService.getMessengerMhaTbls(queryDto);
            return ApiResult.getSuccessInstance(messengerVoList);
        } catch (Throwable e) {
            logger.error("queryMessengerVos exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }



    @DeleteMapping("deleteMha")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MESSENGER_REPLICATION, attr = OperateAttrEnum.DELETE,
            success = "removeMessengerGroupInMha with mhaName: {#mhaName}")
    public ApiResult<Boolean> removeMessengerGroupInMha(@RequestParam String mhaName, @RequestParam String mqType) {
        try {
            logger.info("removeMessengerGroupInMha in mha:{}", mhaName);
            messengerService.removeMessengerGroup(mhaName, MqType.parse(mqType));
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("removeMessengerGroupInMha in mha:" + mhaName, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("queryConfigs")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MqConfigVo>> getAllMqConfigsByMhaName(@RequestParam(name = "mhaName") String mhaName,
                                                                @RequestParam(name = "mqType") String mqType
    ) {
        try {
            List<MqConfigVo> res = messengerService.queryMhaMessengerConfigs(mhaName, MqType.parse(mqType));
            return ApiResult.getSuccessInstance(res);
        } catch (Throwable e) {
            logger.error("getAllMqConfigsByMhaName exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/qmq/bus")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getBuListFromQmq() {
        try {
            return ApiResult.getSuccessInstance(messengerService.getBusFromQmq());
        } catch (Exception e) {
            logger.error("[[tag=qmqInit]]error in getBusFromQmq", e);
            return ApiResult.getFailInstance(null, "error in getBusFromQmq");
        }
    }

    @GetMapping("checkMqConfig")
    @SuppressWarnings("unchecked")
    public ApiResult<MqConfigCheckVo> checkMqConfig(MqConfigDto dto) {
        try {
            logger.info("checkMqConfig :{}", dto);

            dto.validCheckRequest();

            MqConfigCheckVo mqConfigCheckVo = messengerService.checkMqConfig(dto);
            return ApiResult.getSuccessInstance(mqConfigCheckVo);
        } catch (Throwable e) {
            logger.error("checkMqConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("submitConfig")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MESSENGER_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "submitConfig with MqConfigDto: {#dto.toString()}")
    public ApiResult<Boolean> submitConfig(@RequestBody MqConfigDto dto) {
        logger.info("[[tag=mqConfig]] record mqConfig:{}", dto);
        try {
            if (dto.isInsertRequest()) {
                messengerService.processAddMqConfig(dto);
            } else {
                messengerService.processUpdateMqConfig(dto);
            }
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("submitConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("deleteMqConfig")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MESSENGER_REPLICATION, attr = OperateAttrEnum.DELETE,
            success = "deleteMqConfig with MqConfigDeleteRequestDto: {#requestDto.toString()}")
    public ApiResult<Void> deleteMqConfig(@RequestBody MqConfigDeleteRequestDto requestDto) {
        logger.info("deleteMqConfig: {}", requestDto);
        try {
            if (requestDto == null) {
                return ApiResult.getFailInstance("invalid empty input");
            }
            requestDto.validate();
            messengerService.processDeleteMqConfig(requestDto);
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("deleteMqConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("delay")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaReplicationDto>> queryRelatedReplicationDelay(@RequestBody MessengerDelayQueryDto queryDto) {
        List<String> mhas = queryDto.getMhas();
        List<String> dbs = queryDto.getDbs();
        MqType mqType = queryDto.getMqTypeEnum();
        if (CollectionUtils.isEmpty(mhas)) {
            return ApiResult.getSuccessInstance(Collections.emptyList());
        }
        try {
            List<MhaMessengerDto> res;
            List<MhaDelayInfoDto> mhaReplicationDelays;
            if (queryDto.getNoNeedDbAndSrcTime()) {
                res = mhas.stream().map(MhaMessengerDto::from).collect(Collectors.toList());
                mhaReplicationDelays = messengerService.getMhaMessengerDelaysWithoutSrcTime(res, mqType);
            } else {
                res = messengerService.getRelatedMhaMessenger(mhas, dbs, mqType);
                mhaReplicationDelays = messengerService.getMhaMessengerDelays(res, mqType);
            }
            Map<String, MhaDelayInfoDto> delayMap = mhaReplicationDelays.stream().filter(Objects::nonNull).collect(Collectors.toMap(
                            MhaDelayInfoDto::getSrcMha,
                            Function.identity()
                    )
            );
            res.forEach(e -> {
                String key = e.getSrcMha().getName();
                e.setDelayInfoDto(delayMap.get(key));
            });
            return ApiResult.getSuccessInstance(res);

        } catch (Throwable e) {
            logger.error("queryRelatedReplicationDelay error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
