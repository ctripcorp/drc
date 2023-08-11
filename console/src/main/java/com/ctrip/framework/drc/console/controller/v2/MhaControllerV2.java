package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.MhaMachineDto;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/9 20:29
 */
@RestController
@RequestMapping("/api/drc/v2/mha/")
public class MhaControllerV2 {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaServiceV2 mhaServiceV2;

    @GetMapping("replicator")
    public ApiResult<List<String>> getMhaReplicators(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaReplicators(mhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("resources")
    public ApiResult<List<String>> getMhaAvailableResource(@RequestParam String mhaName, @RequestParam int type) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaAvailableResource(mhaName, type));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("uuid")
    public ApiResult<String> getMhaMysqlUuid(@RequestParam String mhaName, @RequestParam String ip,
                                             @RequestParam int port, @RequestParam boolean master) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMysqlUuid(mhaName, ip, port, master));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("machineInfo")
    public ApiResult recordMachineInfo(@RequestBody MhaMachineDto dto) {
        logger.info("record machineInfo : {}", dto);
        try {
            MhaInstanceGroupDto mhaInstanceGroupDto = MhaMachineDto.transferToMhaInstanceGroupDto(dto);
            logger.info("record mha instance: {}", dto);
            Boolean res = mhaServiceV2.recordMhaInstances(mhaInstanceGroupDto);
            return ApiResult.getSuccessInstance(String.format("record mha machine %s result: %s", dto, res));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(String.format("Fail record mha machine %s for %s", dto, t));
        }
    }
}
