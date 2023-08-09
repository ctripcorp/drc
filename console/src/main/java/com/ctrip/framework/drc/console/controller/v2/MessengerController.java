package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController("MessengerControllerV2")
@RequestMapping("/api/drc/v2/messenger/")
public class MessengerController {

    private static final Logger logger = LoggerFactory.getLogger(com.ctrip.framework.drc.console.controller.MessengerController.class);

    @Autowired
    MessengerServiceV2 messengerService;
    @Autowired
    MetaInfoServiceV2 metaInfoServiceV2;

    @GetMapping("all")
    public ApiResult<MessengerVo> getAllMessengerVos() {
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
        } catch (Exception e) {
            logger.error("getAllMessengerVos error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
