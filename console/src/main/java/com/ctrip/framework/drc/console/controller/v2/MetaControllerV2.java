package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.dto.RouteMappingDto;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.MhaDbReplicationRouteDto;
import com.ctrip.framework.drc.console.param.MhaRouteMappingDto;
import com.ctrip.framework.drc.console.param.RouteQueryParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.resource.ProxyService;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.console.vo.v2.ApplierReplicationView;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.drc.core.utils.EncryptUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/6/2 14:47
 */
@RestController
@RequestMapping("/api/drc/v2/meta/")
public class MetaControllerV2 {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaProviderV2 metaProviderV2;

    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private RouteService routeService;
    
    private HeraldService heraldService = ApiContainer.getHeraldServiceImpl();
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @GetMapping
    public String getAllMetaData(
            @RequestParam(value = "refresh", required = false, defaultValue = "false") String refresh,
            @RequestParam(value = "adminToken", required = false, defaultValue = "") String adminToken,
            @RequestParam (value = "heraldToken",required = false, defaultValue = "") String heraldToken) {
        try {
            // check admin
            boolean isAdmin = false;
            if (StringUtils.isNotBlank(adminToken)) {
                String adminTokenDecrypted = EncryptUtils.decryptRawToken(consoleConfig.getDrcAdminToken());
                isAdmin = Objects.equals(adminToken, adminTokenDecrypted);
            }
            // check herald
            boolean heraldValidate = heraldService.heraldValidate(heraldToken);
            if (!heraldValidate && !isAdmin) {
                return null;
            }
            logger.info("[meta] get all, refresh: {}", refresh);
            String drcString;
            if (StringUtils.equals("true", refresh)) {
                drcString = metaProviderV2.getRealtimeDrcString();
            } else {
                drcString = metaProviderV2.getDrcString();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("drc:\n {}", drcString);
            }
            if (StringUtils.isBlank(drcString)) {
                logger.error("get drc fail");
                return null;
            }
            return drcString;
        } catch (Exception e) {
            logger.error("get drc fail", e);
            return null;
        }
    }

    @GetMapping("data/dcs/{dc}")
    public String getDrcStr(
            @PathVariable String dc,
            @RequestParam(value = "refresh", required = false, defaultValue = "false") String refresh,
            @RequestParam(value = "adminToken", required = false, defaultValue = "") String adminToken,
            @RequestParam (value = "heraldToken",required = false, defaultValue = "") String heraldToken) {
        try {
            // check admin
            boolean isAdmin = false;
            if (StringUtils.isNotBlank(adminToken)) {
                String adminTokenDecrypted = EncryptUtils.decryptRawToken(adminToken);
                isAdmin = Objects.equals(consoleConfig.getDrcAdminToken(), adminTokenDecrypted);
            }
            // check herald
            boolean heraldValidate = heraldService.heraldValidate(heraldToken);
            if (!heraldValidate && !isAdmin) {
                return null;
            }
            logger.info("[meta] get meta of dc: {} info, refresh: {}", dc, refresh);
            Drc dcInfo;
            if (StringUtils.equals("true", refresh)) {
                dcInfo = metaProviderV2.getRealtimeDrc(dc);
            } else {
                dcInfo = metaProviderV2.getDrc(dc);
            }
            String dcInfostring = dcInfo.toString();
            if (logger.isDebugEnabled()) {
                logger.debug("get meta of dc: {}, info: \n {}", dc, dcInfostring);
            }
            return dcInfostring;
        } catch (Exception e) {
            logger.error("get dc: {} info fail", dc, e);
            return null;
        }
    }

    @PostMapping("mhaDbReplication/refreshData")
    public ApiResult<Void> refreshMhaDbReplication() {
        try {
            mhaDbReplicationService.refreshMhaReplication();
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("dbApplier/refreshData error", e);
            return ApiResult.getFailInstance(e.getMessage());
        }
    }


    @SuppressWarnings("unchecked")
    @GetMapping("queryConfig/mhaReplicationByName")
    public ApiResult<String> queryMhaReplicationDetailConfig(@RequestParam(name = "srcMha") String srcMhaName,
                                                             @RequestParam(name = "dstMha") String dstMhaName) {
        logger.info("queryReplicationDetailConfig for {} - {}", srcMhaName, dstMhaName);
        try {
            if (StringUtils.isBlank(srcMhaName) || StringUtils.isBlank(dstMhaName)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Invalid input, contact devops!");
            }

            Drc drc = metaInfoServiceV2.getDrcReplicationConfig(srcMhaName, dstMhaName);
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drc.toString()));
        } catch (Throwable e) {
            logger.error("queryMhaReplicationDetailConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @GetMapping("queryConfig/mhaReplication")
    public ApiResult<String> queryMhaReplicationDetailConfig(@RequestParam(name = "replicationId") Long replicationId) {
        logger.info("queryReplicationDetailConfig for {}", replicationId);
        try {
            if (replicationId == null || replicationId <= 0) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Invalid input, contact devops!");
            }

            Drc drc = metaInfoServiceV2.getDrcReplicationConfig(replicationId);
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drc.toString()));
        } catch (Throwable e) {
            logger.error("queryMhaReplicationDetailConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @GetMapping("queryConfig/mhaMessenger")
    public ApiResult<String> queryMhaMessengerDetailConfig(@RequestParam(name = "mhaName") String mhaName,
                                                           @RequestParam(name = "mqType") String mqType) {
        logger.info("queryMhaMessengerDetailConfig for {}", mhaName);
        try {
            MqType mqTypeEnum = MqType.parse(mqType);
            if (StringUtils.isBlank(mhaName) || mqTypeEnum == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Invalid input, contact devops!");
            }

            Drc drc = metaInfoServiceV2.getDrcMessengerConfig(mhaName.trim(), mqTypeEnum);
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drc.toString()));
        } catch (Throwable e) {
            logger.error("queryMhaMessengerDetailConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @GetMapping("queryConfig/mha")
    public ApiResult<String> queryMhaConfig(@RequestParam(name = "mhaName") String mhaName) {
        logger.info("queryMhaConfig for {}", mhaName);
        try {
            if (StringUtils.isBlank(mhaName)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Invalid input, contact devops!");
            }

            Drc drc = metaInfoServiceV2.getDrcMhaConfig(mhaName.trim());
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drc.toString()));
        } catch (Throwable e) {
            logger.error("queryMhaConfig exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("bus/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<BuTbl>> getAllBuTbls() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllBuWithCache());
        } catch (Throwable e) {
            logger.error("getAllBuTbls exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("regions/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RegionTbl>> getAllRegionTbls() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllRegionWithCache());
        } catch (Throwable e) {
            logger.error("getAllRegionTbls exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("region")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createRegion(@RequestParam String regionName) {
        try {
            metaInfoServiceV2.createRegion(regionName);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("createRegion fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("dc")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createDc(@RequestParam String dcName, @RequestParam String regionName) {
        try {
            metaInfoServiceV2.createDc(dcName, regionName);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("createDc fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("dcs/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DcDo>> getAllDcs() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllDcWithCache());
        } catch (Throwable e) {
            logger.error("getAllDcs exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("dbBuCodes/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getAllDbBuCodes() {
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceV2.queryAllDbBuCode());
        } catch (Throwable e) {
            logger.error("getAllDcs exception", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("proxy")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> inputProxy(@RequestParam String dc, @RequestParam String ip) {
        try {
            proxyService.inputProxy(dc, ip);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("proxy/uris")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getProxyUris(@RequestParam String dc, @RequestParam boolean src) {
        try {
            return ApiResult.getSuccessInstance(proxyService.getProxyUris(dc, src));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping("proxy/uris/relay")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getRelayProxyUris() {
        try {
            return ApiResult.getSuccessInstance(proxyService.getRelayProxyUris());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping("route")
    @SuppressWarnings("unchecked")
    public ApiResult<RouteDto> getProxyRoute(@RequestParam long routeId) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRoute(routeId));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @DeleteMapping("route")
    @SuppressWarnings("unchecked")
    public ApiResult<RouteDto> deleteRoute(@RequestParam long routeId) {
        try {
            routeService.deleteRoute(routeId);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @PostMapping("route")
    @SuppressWarnings("unchecked")
    public ApiResult<RouteDto> submitRoute(@RequestBody RouteDto routeDto) {
        try {
            routeService.submitRoute(routeDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @PostMapping("activeRoute")
    @SuppressWarnings("unchecked")
    public ApiResult<RouteDto> activeRoute(@RequestParam long routeId) {
        try {
            routeService.activeRoute(routeId);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @PostMapping("deactivateRoute")
    @SuppressWarnings("unchecked")
    public ApiResult<RouteDto> deactivateRoute(@RequestParam long routeId) {
        try {
            routeService.deactivateRoute(routeId);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @GetMapping(value = "routes")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RouteDto>> getRoutes(RouteQueryParam param) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRoutes(param));
        } catch (Throwable t) {
            logger.error("getRoutes error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping(value = "optionalRoutes")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RouteDto>> getRoutesByRegion(@RequestParam String srcDcName, @RequestParam String dstDcName) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRoutesByRegion(srcDcName, dstDcName));
        } catch (Throwable t) {
            logger.error("getRoutesByRegion error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping(value = "optionalRoutes/dstDc")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RouteDto>> getRoutesByDstDc(@RequestParam String dstDcName) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRoutesByDstRegion(dstDcName));
        } catch (Throwable t) {
            logger.error("getRoutesByDstDc error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @PostMapping(value = "routeMappings")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> submitMhaDbReplicationRoutes(@RequestBody MhaDbReplicationRouteDto routeDto) {
        try {
            routeService.submitMhaDbReplicationRoutes(routeDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            logger.error("submitMhaDbReplicationRoutes error", t);
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @PostMapping(value = "routeMappings/mha")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> submitMhaRoutes(@RequestBody MhaRouteMappingDto routeDto) {
        try {
            routeService.submitMhaRoutes(routeDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            logger.error("submitMhaRoutes error", t);
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @DeleteMapping(value = "routeMappings")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> deleteMhaDbReplicationRoutes(@RequestBody MhaDbReplicationRouteDto routeDto) {
        try {
            routeService.deleteMhaDbReplicationRoutes(routeDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            logger.error("deleteMhaDbReplicationRoutes error", t);
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @DeleteMapping(value = "routeMappings/mha")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> deleteMhaRoutes(@RequestBody MhaDbReplicationRouteDto routeDto) {
        try {
            routeService.deleteMhaRoutes(routeDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable t) {
            logger.error("deleteMhaRoutes error", t);
            return ApiResult.getFailInstance(false, t.getMessage());
        }
    }

    @GetMapping(value = "routeMappings")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RouteMappingDto>> getRouteMappings(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRouteMappings(srcMhaName, dstMhaName));
        } catch (Throwable t) {
            logger.error("getRouteMappings error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping(value = "routeMappings/mha")
    @SuppressWarnings("unchecked")
    public ApiResult<List<RouteMappingDto>> getRouteMappings(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRouteMappings(mhaName));
        } catch (Throwable t) {
            logger.error("getRouteMappings error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping(value = "routeMapping/dbs")
    @SuppressWarnings("unchecked")
    public ApiResult<List<ApplierReplicationView>> getRelatedDbs(@RequestParam long routeId) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRelatedDbs(routeId));
        } catch (Throwable t) {
            logger.error("getRelatedDbs error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }

    @GetMapping(value = "routeMapping/mhas")
    @SuppressWarnings("unchecked")
    public ApiResult<List<ApplierReplicationView>> getRelatedMhas(@RequestParam long routeId) {
        try {
            return ApiResult.getSuccessInstance(routeService.getRelatedMhas(routeId));
        } catch (Throwable t) {
            logger.error("getRelatedMhas error", t);
            return ApiResult.getFailInstance(null, t.getMessage());
        }
    }


}
