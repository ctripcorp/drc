package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class MachineServiceImpl implements MachineService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final LoadingCache<String, Endpoint> cache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public Endpoint load(@Nonnull String mha) throws Exception {
                    return getMasterEndpoint(mha);
                }
            });

    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;


    @Override
    public Endpoint getMasterEndpointCached(String mha) {
        try {
            if (StringUtils.isEmpty(mha)) {
                return null;
            }
            return cache.get(mha);
        } catch (ExecutionException e) {
            logger.error("getDrcReplicationConfig execution exception", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Endpoint getMasterEndpoint(String mha) {
        try {
            if (mha == null) {
                return null;
            }
            MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mha, 0);
            if (mhaTblV2 == null) {
                return this.getMasterEndpointFromDbaApi(mha);
            }
            List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaTblV2.getId(), 0);
            if (CollectionUtils.isEmpty(machineTbls)) {
                return this.getMasterEndpointFromDbaApi(mha);
            }

            return getMaster(mhaTblV2, machineTbls);
        } catch (SQLException e) {
            logger.error("getDrcReplicationConfig sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private Endpoint getMasterEndpointFromDbaApi(String mha) {
        DbaClusterInfoResponse clusterMembersInfo = dbaApiService.getClusterMembersInfo(mha);
        List<MemberInfo> memberlist = clusterMembersInfo.getData().getMemberlist();
        return memberlist.stream()
                .filter(memberInfo -> memberInfo.getRole().toLowerCase().contains("master"))
                .map(e -> new MySqlEndpoint(
                        e.getService_ip(),
                        e.getDns_port(),
                        monitorTableSourceProvider.getMonitorUserVal(),
                        monitorTableSourceProvider.getMonitorPasswordVal(),
                        BooleanEnum.TRUE.isValue()
                ))
                .findFirst().orElse(null);
    }

    public Endpoint getMaster(MhaTblV2 mhaTblV2, List<MachineTbl> machineInfo) {
        for (MachineTbl machineTbl : machineInfo) {
            if (machineTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
                return new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }
}
