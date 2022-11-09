package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.console.vo.MessengerVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import qunar.api.open.annotation.RequestParam;

import java.sql.SQLException;
import java.util.List;


/**
 * @ClassName MessengerController
 * @Author haodongPan
 * @Date 2022/10/25 15:37
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/messenger/")
public class MessengerController {

    private static final Logger logger = LoggerFactory.getLogger(MessengerController.class); 

    @Autowired MessengerService messengerService;

    @GetMapping("all")
    public ApiResult getAllMessengerVos() {
        try {
            List<MessengerVo> vos = messengerService.getAllMessengerVos();
            return ApiResult.getSuccessInstance(vos);
        } catch (SQLException e) {
            logger.error("[[tag=messenger]] sql error in getAllMessengerVos",e);
            return ApiResult.getFailInstance(null,"query error");
        }

    }

    @DeleteMapping()
    public ApiResult removeMessengerGroupInMha(@RequestParam String mhaName) {
        try {
            logger.info("[[tag=messenger]] removeMessengerGroup in mha:{}",mhaName);
            return ApiResult.getSuccessInstance(null,messengerService.removeMessengerGroup(mhaName));
        } catch (SQLException e) {
            logger.error("[[tag=messenger]] sql error in removeMessengerGroup in mha:{}",mhaName,e);
            return ApiResult.getFailInstance(null,"query error");
        }

    }
    
    
    @GetMapping("mqConfigs/{messengerGroupId}")
    public ApiResult getAllMqConfigsByMessengerGroupId(@PathVariable Long messengerGroupId) {
        try {
            return ApiResult.getSuccessInstance(messengerService.getMqConfigVos(messengerGroupId));
        } catch (SQLException e) {
            logger.error("[[tag=mqConfig]] sql error in getMqConfigVos",e);
            return ApiResult.getFailInstance(null,"query error");
        }
    }

    @PostMapping("mqConfig")
    public ApiResult inputMqConfig(@RequestBody MqConfigDto dto) {
        try {
            logger.info("[[tag=mqConfig]] record mqConfig:{}",dto);
            if (dto.getId() == 0L ) {
                return ApiResult.getSuccessInstance(null,messengerService.processAddMqConfig(dto));
            } else {
                return ApiResult.getSuccessInstance(null,messengerService.processUpdateMqConfig(dto));
            }
        } catch (Exception e) {
            logger.error("[[tag=mqConfig]]  error in recordMqConfig",e);
            return ApiResult.getFailInstance(null,"record error");
        }
    }

    @DeleteMapping("mqConfig/{mqConfigId}")
    public ApiResult deleteMqConfig(@PathVariable Long mqConfigId) {
        try {
            logger.info("[[tag=mqConfig]] delete mqConfig id:{}",mqConfigId);
            if (mqConfigId == 0L ) {
                return ApiResult.getFailInstance(null,"illegal argument");
            } else {
                return ApiResult.getSuccessInstance(null,messengerService.processDeleteMqConfig(mqConfigId));
            }
        } catch (Exception e) {
            logger.error("[[tag=mqConfig]]  error in deleteMqConfig",e);
            return ApiResult.getFailInstance(null,"delete error");
        }
    }
    

    @PostMapping("qmq/bus")
    public ApiResult getBuListFromQmq() {
        try {
            return ApiResult.getSuccessInstance(messengerService.getBusFromQmq());
        } catch (Exception e) {
            logger.error("[[tag=qmqInit]]error in getBusFromQmq",e);
            return ApiResult.getFailInstance(null,"error in getBusFromQmq");
        }
    }
    
}
