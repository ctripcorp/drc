package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.TableSchemaListApiResult;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class DrcBuildServiceImpl implements DrcBuildService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DalUtils dalUtils = DalUtils.getInstance();
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;


    @Override
    @PossibleRemote(path = "/api/drc/v1/build/queryTables")
    public List<String> queryTablesWithNameFilter(String mha, String nameFilter) {
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("queryTablesWithNameFilter from mha: {},db not exist", mha);
            return new ArrayList<>();
        }
        return MySqlUtils.queryTablesWithFilter(endpoint, nameFilter);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v1/build/dataMedia/check" ,responseType = TableSchemaListApiResult.class)
    public List<MySqlUtils.TableSchemaName> getMatchTable(String namespace, String name,
                                                          String mhaName, Integer type) {
        logger.info("[[tag=matchTable]] get {}.{} from {} ",namespace,name,mhaName);
        Endpoint mySqlEndpoint = dbClusterSourceProvider.getMasterEndpoint(mhaName);
        if (mySqlEndpoint != null) {
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." +  name);
            return MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
        } else {
            throw new IllegalArgumentException("no machine find for" + mhaName);
        }
    }

    protected List<String> removeReplicatorInstances(List<String> replicatorIpsToBeRemoved, String mhaName, Long replicatorGroupId, List<ResourceTbl> resourceTbls, List<ReplicatorTbl> replicatorTbls) {
        logger.info("[[mha={}]]try remove replicators {}", mhaName, replicatorIpsToBeRemoved);
        List<String> replicatorInstancesRemoved = Lists.newArrayList();
        if(replicatorIpsToBeRemoved.size() != 0) {
            for(String ip : replicatorIpsToBeRemoved) {
                logger.info("[[mha={}]]remove replicator: {}", mhaName, ip);
                ResourceTbl resourceTbl = resourceTbls.stream().filter(p -> ip.equalsIgnoreCase(p.getIp())).findFirst().orElse(null);
                if(null == resourceTbl) {
                    logger.info("[[mha={}]]UNLIKELY-replicator resource({}) should already be loaded", mhaName, ip);
                    continue;
                }
                // find the replicator and logically remove it
                ReplicatorTbl replicatorTbl = replicatorTbls.stream().filter(p -> (replicatorGroupId.equals(p.getRelicatorGroupId())) && resourceTbl.getId().equals(p.getResourceId())).findFirst().orElse(null);
                try {
                    assert null != replicatorTbl;
                    replicatorTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    dalUtils.getReplicatorTblDao().update(replicatorTbl);
                    replicatorInstancesRemoved.add(ip+':'+replicatorTbl.getApplierPort());
                } catch (Throwable t) {
                    logger.error("[[mha={}]]Failed remove replicator {}", mhaName, ip, t);
                }
            }
        }
        return replicatorInstancesRemoved;
    }
    public String getGtidInit(MhaTbl mhaTbl) throws SQLException {
        if (mhaTbl == null){
            return "";
        }
        Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
        String mhaDcName = dalUtils.getDcNameByDcId(mhaTbl.getDcId());
        String regionForDc = consoleConfig.getRegionForDc(mhaDcName);
        if(publicCloudRegion.contains(regionForDc.toLowerCase())) {
            return "";
        }

        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mhaTbl.getMhaName());
        return MySqlUtils.getUnionExecutedGtid(endpoint);
    }


    public String submitProxyRouteConfig(RouteDto routeDto) {
        try {
            Long routeOrgId = StringUtils.isBlank(routeDto.getRouteOrgName()) ? 0L : dalUtils.getId(TableEnum.BU_TABLE, routeDto.getRouteOrgName());
            Long srcDcId = dalUtils.getId(TableEnum.DC_TABLE, routeDto.getSrcDcName());
            Long dstDcId = dalUtils.getId(TableEnum.DC_TABLE, routeDto.getDstDcName());
            List<Long> srcProxyIds = Lists.newArrayList();
            List<Long> relayProxyIds = Lists.newArrayList();
            List<Long> dstProxyIds = Lists.newArrayList();
            for(String proxyUri : routeDto.getSrcProxyUris()) {
                srcProxyIds.add(dalUtils.getId(TableEnum.PROXY_TABLE, proxyUri));
            }
            for (String proxyUri : routeDto.getRelayProxyUris()) {
                relayProxyIds.add(dalUtils.getId(TableEnum.PROXY_TABLE, proxyUri));
            }
            for(String proxyUri : routeDto.getDstProxyUris()) {
                dstProxyIds.add(dalUtils.getId(TableEnum.PROXY_TABLE, proxyUri));
            }
            dalUtils.updateOrCreateRoute(routeOrgId, srcDcId, dstDcId, StringUtils.join(srcProxyIds, ","), StringUtils.join(relayProxyIds, ","), StringUtils.join(dstProxyIds, ","), routeDto.getTag(),routeDto.getDeleted());
            return "update proxy route succeeded";
        } catch (SQLException e) {
            logger.error("update proxy route failed, ", e);
            return "update proxy route failed";
        }
    }


}
