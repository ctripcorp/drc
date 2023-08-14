package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
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

    @GetMapping("all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MessengerVo>> getAllMessengerVos() {
        try {
            List<MhaTblV2> messengerMhaTbls = messengerService.getAllMessengerMhaTbls();

            List<BuTbl> buTbls = metaInfoServiceV2.queryAllBuWithCache();
            Map<Long, BuTbl> buMap = buTbls.stream().collect(Collectors.toMap(BuTbl::getId, Function.identity()));

            // convert
            List<MessengerVo> messengerVoList = messengerMhaTbls.stream().map(mhaDto -> {
                MessengerVo messengerVo = new MessengerVo();
                messengerVo.setMhaName(mhaDto.getMhaName());
                BuTbl buTbl = buMap.get(mhaDto.getBuId());
                if (buTbl != null) {
                    messengerVo.setBu(buTbl.getBuName());
                }
                messengerVo.setMonitorSwitch(mhaDto.getMonitorSwitch());
                return messengerVo;
            }).collect(Collectors.toList());
            return ApiResult.getSuccessInstance(messengerVoList);
        } catch (Throwable e) {
            logger.error("getAllMessengerVos exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("queryConfigs")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MqConfigVo>> getAllMqConfigsByMhaName(@RequestParam(name = "mhaName") String mhaName) {
        try {
            List<MqConfigVo> res = messengerService.queryMhaMessengerConfigs(mhaName);
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
    public ApiResult<Boolean> submitConfig(@RequestBody MqConfigDto dto) {
        try {
            logger.info("[[tag=mqConfig]] record mqConfig:{}", dto);
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
    public ApiResult<Void> deleteMqConfig(@RequestBody MqConfigDeleteRequestDto requestDto) {
        try {
            logger.info("deleteMqConfig: {}", requestDto);
            if (requestDto == null) {
                return ApiResult.getFailInstance("invalid empty input");
            }
            messengerService.deleteDbReplicationForMq(requestDto.getMhaName(), requestDto.getDbReplicationIdList());
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("deleteMqConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("initGtid")
    public ApiResult getMessengerExecutedGtid(@RequestParam(value = "mhaName") String mhaName) {
        logger.info("[[tag=messenger]] getMessengerExecutedGtid for mha: {} ", mhaName);
        try {
            return ApiResult.getSuccessInstance(messengerService.getMessengerGtidExecuted(mhaName));
        } catch (Exception e) {
            logger.error("[[tag=messenger]] fail in getMessengerExecutedGtid for mha: {} ", mhaName, e);
            return ApiResult.getSuccessInstance(null);
        }
    }
}
