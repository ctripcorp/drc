package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.*;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
@RestController
@RequestMapping("/api/drc/v1/meta/")
public class MetaController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private TransferServiceImpl transferService;

    @Autowired
    private DrcBuildServiceImpl drcBuildService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Autowired
    private DbClusterSourceProvider sourceProvider;


    @GetMapping("groups/all")
    public ApiResult getAllMhaGroups() {
        logger.info("[meta] get all mha groups info");
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getAllMhaGroups());
        } catch (Exception e) {
            logger.error("[meta] get all mha groups info", e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("orderedGroups/all")
    public ApiResult getAllOrderedGroups() {
        logger.info("[meta] get all mha groups info");
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getAllOrderedGroupPairs());
        } catch (Exception e) {
            logger.error("[meta] get all mha groups info", e);
            return ApiResult.getFailInstance(null);
        }
    }


    @GetMapping("orderedDeletedGroups/all")
    public ApiResult getAllDeletedOrderedGroups() {
        logger.info("[meat] get all deleted mha groups info");
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getAllOrderedDeletedGroupPairs());
        } catch (Exception e) {
            logger.info("[meat] get all deleted mha groups info", e);
            return ApiResult.getFailInstance(null);
        }
    }

    /**
     * curl -H "Content-Type:application/json" -X PUT -d "<?xml version=\"1.0\" encoding=\"utf-8\"?> <drc> <dc id=\"shaoy\"> <clusterManager ip=\"127.0.0.1\" port=\"8080\" master=\"true\"/> <zkServer address=\"127.0.0.1:2181\"/> <dbClusters> </dbClusters> </dc> <dc id=\"sharb\"> <clusterManager ip=\"127.0.0.2\" port=\"8080\" master=\"true\"/> <zkServer address=\"127.0.0.1:2181\"/> <dbClusters> </dbClusters> </dc> </drc>" 'http://127.0.0.1:8080/api/drc/v1/meta'
     *
     * @param metaData as xml String
     * @return
     */
    @PutMapping
    public ApiResult loadMetaData(@RequestBody(required = false) String metaData) {
        logger.info("[meta] load xml: {}", metaData);
        try {
            transferService.loadMetaData(metaData);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[meta] load error, ", e);
            return ApiResult.getFailInstance(false);
        }
    }

    @PutMapping("one")
    public ApiResult loadOneMetaData(@RequestBody(required = false) String oneMetaData) {
        logger.info("[meta] load one meta xml: {}", oneMetaData);
        try {
            transferService.loadOneMetaData(oneMetaData);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[meta] load error, ", e);
            return ApiResult.getFailInstance(false);
        }
    }

    /**
     * curl -H "Content-Type:application/json" -X PUT 'http://127.0.0.1:8080/api/drc/v1/meta/auto'
     *
     * @return
     */
    @PutMapping("auto")
    public ApiResult autoLoadMetaData() {
        String metaData = monitorTableSourceProvider.getMetaData();
        logger.info("[meta] auto load xml from QConfig: {}", metaData);
        try {
            transferService.loadMetaData(metaData);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[meta] load error, ", e);
            return ApiResult.getFailInstance(false);
        }
    }

    @PostMapping("group/status/{status}")
    public ApiResult changeMhaGroupStatus(@RequestBody MhaGroupPair mhaGroupPair, @PathVariable Integer status) {
        logger.info("[meta] change mha group status {}-{} to {}", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), status);
        try {
            drcMaintenanceService.changeMhaGroupStatus(mhaGroupPair, status);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[meta] change mha group status {}-{} to {}", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), status, e);
            return ApiResult.getFailInstance(false);
        }
    }

    @GetMapping
    public String getAllMetaData() {
        logger.info("[meta] get all");
        return sourceProvider.getDrcString();
    }

    /**
     * Deprecated
     */
    @GetMapping("local")
    public String getLocalDrcStr() {
        logger.info("[meta] local");
        try {
            String localMetaData = sourceProvider.getLocalDrc().toString();
            META_LOGGER.debug("local meta: {}", localMetaData);
            return localMetaData;
        } catch (Exception e) {
            logger.error("[meta] fail get local, ", e);
            return null;
        }
    }

    @GetMapping("data/dcs/{dc}")
    public String getDrcStr(@PathVariable String dc) {
        logger.info("[meta] get meta for {}", dc);
        try {
            String metaData = sourceProvider.getDrc(dc).toString();
            META_LOGGER.debug("meta in {}: {}", dc, metaData);
            return metaData;
        } catch (Exception e) {
            logger.error("[meta] fail get meta in {}, ", dc, e);
            return null;
        }
    }

    @GetMapping("mhas/{mha}/resources/all/types/{type}")
    public ApiResult getResourcesInDcOfMha(@PathVariable String mha, @PathVariable String type) {
        logger.info("[meta] get {} resources in dc of mha({})", type, mha);
        return ApiResult.getSuccessInstance(metaInfoService.getResourcesInDcOfMha(mha, type));
    }

    //just for dc migration
    @GetMapping("resources")
    public ApiResult getResources(@RequestParam(value = "type") String type) {
        logger.info("[meta] get resources, type is: {}", type);
        return ApiResult.getSuccessInstance(metaInfoService.getResources(type));
    }

    //just for dc migration
    @PutMapping("mhas/{mha}")
    public ApiResult updateMhaDc(@PathVariable String mha, @RequestParam(value = "dcName") String dcName) {
        logger.info("[meta] update mha {} dcName as {}", mha, dcName);
        return ApiResult.getSuccessInstance(metaInfoService.updateMhaDc(mha, dcName));
    }

    @GetMapping("includeddbs")
    public ApiResult getIncludedDbs(@RequestParam(value = "localMha") String localMha,
                                    @RequestParam(value = "remoteMha") String remoteMha) {
        logger.info("[meta] get includedDbs setting for appliers being used by {}<-{}", localMha, remoteMha);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getIncludedDbs(localMha, remoteMha));
        } catch (SQLException e) {
            logger.error("[meta] Fail get includedDbs setting for appliers being used by {}<-{}", localMha, remoteMha);
            return ApiResult.getSuccessInstance(null);
        }
    }

    @GetMapping("namefilter")
    public ApiResult getNameFilter(@RequestParam(value = "localMha") String localMha,
                                   @RequestParam(value = "remoteMha") String remoteMha) {
        logger.info("[meta] get nameFilter setting for appliers being used by {}<-{}", localMha, remoteMha);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getNameFilter(localMha, remoteMha));
        } catch (SQLException e) {
            logger.error("[meta] Fail get nameFilter setting for appliers being used by {}<-{}", localMha, remoteMha);
            return ApiResult.getSuccessInstance(null);
        }
    }

    @GetMapping("namemapping")
    public ApiResult getNameMapping(@RequestParam(value = "localMha") String localMha,
                                   @RequestParam(value = "remoteMha") String remoteMha) {
        logger.info("[meta] get nameMapping setting for appliers being used by {}<-{}", localMha, remoteMha);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getNameMapping(localMha, remoteMha));
        } catch (SQLException e) {
            logger.error("[meta] Fail get nameMapping setting for appliers being used by {}<-{}", localMha, remoteMha);
            return ApiResult.getSuccessInstance(null);
        }
    }

    @GetMapping("applymode")
    public ApiResult getApplyMode(@RequestParam(value = "localMha") String localMha,
                                  @RequestParam(value = "remoteMha") String remoteMha) {
        logger.info("[meta] get applyMode setting for appliers being used by {}<-{}", localMha, remoteMha);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getApplyMode(localMha, remoteMha));
        } catch (SQLException e) {
            logger.error("[meta] Fail get applyMode setting for appliers being used by {}<-{}", localMha, remoteMha);
            return ApiResult.getSuccessInstance(null);
        }
    }

    @GetMapping("dcs/{dc}/resources/types/{type}")
    public ApiResult getResources(@PathVariable String dc, @PathVariable String type) {
        logger.info("[meta] get {} resources in {}", type, dc);
        return ApiResult.getSuccessInstance(metaInfoServiceTwo.getResources(dc, type));
    }

    @GetMapping("resources/using/types/{type}")
    public ApiResult getResourcesInUse(@PathVariable String type,
                                       @RequestParam(value = "localMha", defaultValue = "") String localMha,
                                       @RequestParam(value = "remoteMha", defaultValue = "") String remoteMha) {
        logger.info("[meta] get {} resources being used by ({}<-{})", type, localMha, remoteMha);
        return ApiResult.getSuccessInstance(metaInfoService.getResourcesInUse(localMha, remoteMha, type));
    }

    @GetMapping("resources/ip/{ip:.+}")
    public ApiResult getInstances(@PathVariable String ip) {
        logger.info("[meta] get {} instances", ip);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getReplicatorInstances(ip));
        } catch (Exception e) {
            logger.error("[meta] get {} instances", ip, e);
        }
        return ApiResult.getFailInstance(null);
    }

    /**
     * submit to configure DRC settings for mha pairs
     *
     * @param metaProposalDto
     * @return
     */
    @PostMapping("config")
    public ApiResult submitConfig(@RequestBody MetaProposalDto metaProposalDto) {
        logger.info("[meta] submit meta config for {}", metaProposalDto);
        try {
            return ApiResult.getSuccessInstance(drcBuildService.submitConfig(metaProposalDto));
        } catch (Exception e) {
            logger.error("[meta] submit meta config for {}", metaProposalDto, e);
        }
        return ApiResult.getFailInstance(null);
    }

    @PostMapping("config/preCheck")
    public ApiResult preCheckBeforeBuildDrc(@RequestBody MetaProposalDto metaProposalDto) {
        logger.info("[meta] preCheck meta config for  {}", metaProposalDto);
        try {
            return ApiResult.getSuccessInstance(drcBuildService.preCheckBeforeBuild(metaProposalDto));
        } catch (SQLException e) {
            logger.error("[meta] preCheck meta config for {}",metaProposalDto,e);
        }
        return ApiResult.getFailInstance(null);
    }

    @DeleteMapping("config/remove/mhas/{mhas}")
    public ApiResult removeConfig(@PathVariable String mhas) {
        logger.info("[meta] remove meta config for {}", mhas);
        try {
            String[] mhaArr = mhas.split(",");
            if (mhaArr.length == 2) {
                transferService.removeConfig(mhaArr[0], mhaArr[1]);
                return ApiResult.getSuccessInstance(true);
            }
        } catch (Exception e) {
            logger.error("[meta] remove meta config for {}", mhas, e);
        }
        return ApiResult.getFailInstance(false);
    }

    @GetMapping("config/mhas/{mhas}")
    public ApiResult getConfig(@PathVariable String mhas) {
        logger.info("[meta] query meta config for {}", mhas);
        try {
            String[] mhaArr = mhas.split(",");
            return ApiResult.getSuccessInstance(metaInfoService.getXmlConfiguration(mhaArr[0], mhaArr[1]));
        } catch (Exception e) {
            logger.error("[meta] meta config for {}", mhas, e);
        }
        return ApiResult.getFailInstance(null);
    }

    @GetMapping("config/deletedMhas/{mhas}")
    public ApiResult getDeletedConfig(@PathVariable String mhas) {
        logger.info("[meta] query meta config for {}", mhas);
        try {
            String[] mhaArr = mhas.split(",");
            return ApiResult.getSuccessInstance(metaInfoService.getXmlConfiguration(mhaArr[0], mhaArr[1], BooleanEnum.TRUE));
        } catch (Exception e) {
            logger.error("[meta] meta config for {}", mhas, e);
        }
        return ApiResult.getFailInstance(null);
    }

    @PostMapping("config/recoverMhas/{mhas}")
    public ApiResult recoverDrc(@PathVariable String mhas) {
        logger.info("[meta] recover meta config for {}", mhas);
        try {
            String[] mhaArr = mhas.split(",");
            transferService.recoverDeletedDrc(mhaArr[0], mhaArr[1]);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[meta] meta recover config for {}", mhas, e);
        }
        return ApiResult.getFailInstance(null);
    }

    @PostMapping("resource/ips/{ip:.+}/dcs/{dc}/descriptions/{description}")
    public ApiResult inputResource(@PathVariable String ip, @PathVariable String dc, @PathVariable String description) {
        logger.info("[meta] load resource {} in {} with desc {}", ip, dc, description);
        try {
            return ApiResult.getSuccessInstance(drcMaintenanceService.inputResource(ip, dc, description));
        } catch (Exception e) {
            logger.error("[meta] error load resource {} in {} with desc {}, ", ip, dc, description, e);
            return ApiResult.getFailInstance(e);
        }
    }

    @DeleteMapping("resource/ips/{ip:.+}")
    public ApiResult deleteResource(@PathVariable String ip) {
        logger.info("[meta] delete resource {}", ip);
        try {
            return ApiResult.getSuccessInstance(drcMaintenanceService.deleteResource(ip));
        } catch (Exception e) {
            logger.error("[meta] error delete resource {}, ", ip, e);
            return ApiResult.getFailInstance(e);
        }
    }

    @DeleteMapping("machine/ips/{ip:.+}")
    public ApiResult deleteMachine(@PathVariable String ip) {
        logger.info("[meta] delete machine {}", ip);
        try {
            return ApiResult.getSuccessInstance(drcMaintenanceService.deleteMachine(ip));
        } catch (Exception e) {
            logger.error("[meta] error delete machine {}, ", ip, e);
            return ApiResult.getFailInstance(e);
        }
    }

    @DeleteMapping(value = "routes/proxy")
    public ApiResult deleteProxyRoute(@RequestParam(value = "routeOrgName") String routeOrgName,
                                      @RequestParam(value = "srcDcName") String srcDcName,
                                      @RequestParam(value = "dstDcName") String dstDcName,
                                      @RequestParam(value = "tag") String tag) {
        logger.info("[meta] delete proxy route for {}-{},{}->{}", routeOrgName, tag, srcDcName, dstDcName);
        return drcMaintenanceService.deleteRoute(routeOrgName, srcDcName, dstDcName, tag);
    }

    @GetMapping(value = "routes")
    public ApiResult getProxyRoutes(@RequestParam(value = "routeOrgName", required = false) String routeOrgName,
                                    @RequestParam(value = "srcDcName", required = false) String srcDcName,
                                    @RequestParam(value = "dstDcName", required = false) String dstDcName,
                                    @RequestParam(value = "tag", required = false) String tag) {
        logger.info("[meta] get proxy routes for {}-{},{}->{}", routeOrgName, tag, srcDcName, dstDcName);
        List<RouteDto> routeDtoList = metaInfoService.getRoutes(routeOrgName, srcDcName, dstDcName, tag);
        return ApiResult.getSuccessInstance(routeDtoList);
    }

    @PostMapping(value = "routes")
    public ApiResult submitProxyRouteConfig(@RequestBody RouteDto routeDto) {
        logger.info("[meta] submit proxy route config for {}", routeDto);
        return ApiResult.getSuccessInstance(drcBuildService.submitProxyRouteConfig(routeDto));
    }

    @PostMapping("dcs/{dc}")
    public ApiResult inputDc(@PathVariable String dc) {
        logger.info("[meta] input dc {}", dc);
        return drcMaintenanceService.inputDc(dc);
    }

    @PostMapping("orgs/{org}")
    public ApiResult inputBu(@PathVariable String org) {
        logger.info("[meta] input bu {}", org);
        return drcMaintenanceService.inputBu(org);
    }

    @PostMapping("proxy")
    public ApiResult inputProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] load proxy: {}", proxyDto);
        return drcMaintenanceService.inputProxy(proxyDto);
    }

    @DeleteMapping("proxy")
    public ApiResult deleteProxy(@RequestBody ProxyDto proxyDto) {
        logger.info("[meta] delete proxy: {}", proxyDto);
        return drcMaintenanceService.deleteProxy(proxyDto);
    }

    @GetMapping("proxy/uris/dcs/{dc}")
    public ApiResult getProxyUris(@PathVariable String dc) {
        logger.info("[meta] get proxy uris for {}", dc);
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceTwo.getProxyUris(dc));
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

    @GetMapping("proxy/uris")
    public ApiResult getAllProxyUris() {
        logger.info("[meta] get all proxy uris");
        try {
            return ApiResult.getSuccessInstance(metaInfoServiceTwo.getAllProxyUris());
        } catch (Throwable t) {
            return ApiResult.getFailInstance(t);
        }
    }

    @PostMapping("group/mapping/group/{groupId}/mha/{mhaId}")
    public ApiResult inputGroupMapping(@PathVariable long groupId, @PathVariable long mhaId) {
        logger.info("[meta] load group mapping: {}-{}", groupId, mhaId);
        try {
            return ApiResult.getSuccessInstance(drcMaintenanceService.inputGroupMapping(groupId, mhaId));
        } catch (Throwable t) {
            logger.error("[meta] fail load group mapping: {}-{}", groupId, mhaId);
            return ApiResult.getFailInstance(t);
        }
    }

    @DeleteMapping("group/mapping/group/{groupId}/mha/{mhaId}")
    public ApiResult deleteProxy(@PathVariable Long groupId, @PathVariable Long mhaId) {
        logger.info("[meta] delete group mapping: {}-{}", groupId, mhaId);
        try {
            return ApiResult.getSuccessInstance(drcMaintenanceService.deleteGroupMapping(groupId, mhaId));
        } catch (Throwable t) {
            logger.error("[meta] fail delete group mapping: {}-{}", groupId, mhaId);
            return ApiResult.getFailInstance(t);
        }
    }

    @GetMapping("mhas")
    public ApiResult queryMhas(@RequestParam(value = "dcName", required = false) String dcName) {
        logger.info("[meta] query mha info, dc name is: {}", dcName);
        try {
            return ApiResult.getSuccessInstance(metaInfoService.getMhas(dcName));
        } catch (SQLException e) {
            return ApiResult.getFailInstance(e);
        }
    }
}
