package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.MhaMachineDto;
import com.ctrip.framework.drc.console.dto.v3.MhaMessengerDto;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.v2.MhaQueryParam;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DisposableFeature;
import com.ctrip.framework.drc.console.vo.check.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.response.GtidCheckResVo;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.mq.MqType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/8/8 14:54
 */
@RestController
@RequestMapping("/api/drc/v2/mha/")
public class MhaControllerV2 {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

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

    @GetMapping("gtid/executed")
    public ApiResult getMhaExecutedGtid(@RequestParam String mha) {
        try {
            String unionGtid = mysqlServiceV2.getMhaExecutedGtid(mha);
            if (StringUtils.isEmpty(unionGtid)) {
                return ApiResult.getFailInstance(null, "result is empty");
            } else {
                return ApiResult.getSuccessInstance(unionGtid);
            }
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaExecutedGtid from mha: {}", mha, e);
            return ApiResult.getFailInstance(e, "unexpected exception");
        }
    }

    @GetMapping("gtid/purged")
    public ApiResult getMhaPurgedGtid(@RequestParam String mha) {
        try {
            String purgedGtid = mysqlServiceV2.getMhaPurgedGtid(mha);
            return ApiResult.getSuccessInstance(purgedGtid);
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaPurgedGtid from mha: {},configGtid:{}", mha, e);
            return ApiResult.getFailInstance(e, "unexpected exception");
        }
    }

    @GetMapping("gtid/applied")
    public ApiResult<String> getMhaAppliedGtid(@RequestParam String mha) {
        try {
            String appliedGtid = mysqlServiceV2.getMhaAppliedGtid(mha);
            return ApiResult.getSuccessInstance(appliedGtid);
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaAppliedGtid from mha: {},configGtid:{}", mha, e);
            return ApiResult.getFailInstance(e, "unexpected exception");
        }
    }

    @GetMapping("db/gtid/applied")
    public ApiResult<Map<String, String>> getMhaDbAppliedGtid(@RequestParam String mha) {
        try {
            Map<String, String> mhaDbAppliedGtid = mysqlServiceV2.getMhaDbAppliedGtid(mha);
            return ApiResult.getSuccessInstance(mhaDbAppliedGtid);
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaDbAppliedGtid from mha: {},configGtid:{}", mha, e);
            return ApiResult.getFailInstance(e, "unexpected exception");
        }
    }

    @GetMapping("gtid/checkResult")
    public ApiResult getGtidCheckResult(@RequestParam String mha, @RequestParam String configGtid) {
        try {
            String purgedGtid = mysqlServiceV2.getMhaPurgedGtid(mha);
            GtidSet purgedGtidSet = new GtidSet(purgedGtid);
            boolean legal = purgedGtidSet.isContainedWithin(new GtidSet(configGtid));
            GtidCheckResVo resVo = new GtidCheckResVo(legal, purgedGtid);
            return ApiResult.getSuccessInstance(resVo);
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getGtidCheckResult from mha: {},configGtid:{}", mha, configGtid, e);
            return ApiResult.getFailInstance(e, "unexpected exception");
        }
    }

    @GetMapping("messenger")
    public ApiResult<MhaMessengerDto> getMhaMessengers(@RequestParam(name = "mhaName") String mhaName,
                                                       @RequestParam(name = "mqType") String mqType
    ) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaMessengers(mhaName, MqType.parse(mqType)));
        } catch (Throwable e) {
            logger.error("getMhaMessengers for {} exception ", mhaName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("replicatorIps/check")
    public ApiResult<DrcBuildPreCheckVo> preCheckBeforeBuildDrc(MessengerMetaDto dto) {
        logger.info("[meta] preCheck meta config for  {}", dto);
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.preCheckBeReplicatorIps(dto.getMhaName(), dto.getReplicatorIps()));
        } catch (Throwable e) {
            logger.error("[meta] preCheck meta config for {}", dto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("tag")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "updateMhaTag with mhaName: {#mhaReplicationId},tag: {#tag}")
    public ApiResult<Boolean> updateMhaTag(@RequestParam String mhaName, @RequestParam String tag) {
        try {
            mhaServiceV2.updateMhaTag(mhaName, tag);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("dc")
    public ApiResult<String> getMhaDc(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhaDc(mhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }


    @GetMapping("shouldOffline")
    public ApiResult getMhasShouldOffline() {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.queryMhasWithOutDrc());
        } catch (Exception e) {
            logger.error("getMhasShouldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping("mhaName")
    public ApiResult deleteMhasShuoldOffline(@RequestBody List<String> mhas) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.offlineMhasWithOutDrc(mhas));
        } catch (Exception e) {
            logger.error("deleteMhasShuoldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("shouldOfflineV2")
    @DisposableFeature
    public ApiResult getMhasShouldOfflineV2(@RequestParam(required = true) Boolean checkDbReplication) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.getMhasWithoutDrcReplication(checkDbReplication));
        } catch (Exception e) {
            logger.error("getMhasShouldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
    @DeleteMapping("mhaWithoutDrcReplication")
    @DisposableFeature
    public ApiResult deleteReplicators(@RequestBody List<String> mhas) {
        try {
            if (CollectionUtils.isEmpty(mhas)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "empty input ");
            }
            return ApiResult.getSuccessInstance(mhaServiceV2.offlineMhasWithOutReplication(mhas));
        } catch (Exception e) {
            logger.error("deleteMachineShouldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }


    @GetMapping("machine/shouldOffline")
    public ApiResult getMachineShouldOffline() {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.queryMachineWithOutMha());
        } catch (Exception e) {
            logger.error("getMachineShouldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping("machine/offline")
    public ApiResult deleteMachineShouldOffline(@RequestBody List<Long> machineIds) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.offlineMachineWithOutMha(machineIds));
        } catch (Exception e) {
            logger.error("deleteMachineShouldOffline error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping()
    public ApiResult<List<MhaTblV2>> queryMhas(MhaQueryParam param) {
        try {
            ApiResult result = ApiResult.getSuccessInstance(mhaServiceV2.queryMhas(param));
            result.setPageReq(param.getPageReq());
            return result;
        } catch (Exception e) {
            logger.error("queryMhas error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("columnDefault")
    public ApiResult findColumnDefaultValueLengthGt251(@RequestParam String mha) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.findColumnDefaultValueLengthGt251(mha));
        } catch (Exception e) {
            logger.error("findColumnDefaultValueLengthGt251 error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("columnDefault/list")
    public ApiResult findColumnDefaultValueLengthGt251(@RequestBody List<String> mhas) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.findColumnDefaultValueLengthGt251(mhas));
        } catch (Exception e) {
            logger.error("findColumnDefaultValueLengthGt251 error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("columnDefault/listAll")
    public ApiResult findAllColumnDefaultValueLengthGt251(@RequestParam int batch) {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.findAllColumnDefaultValueLengthGt251(batch));
        } catch (Exception e) {
            logger.error("findAllColumnDefaultValueLengthGt251 error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("createDrcMessengerGtidTbl")
    public ApiResult createDrcMessengerGtidTbl() {
        try {
            return ApiResult.getSuccessInstance(mhaServiceV2.createDrcMessengerGtidTbl());
        } catch (Exception e) {
            logger.error("createDrcMessengerGtidTbl error", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

}
