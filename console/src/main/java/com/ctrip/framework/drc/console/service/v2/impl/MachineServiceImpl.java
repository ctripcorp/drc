package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
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

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.ArrayList;
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
                public Endpoint load(@NotNull String mha) {
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
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private AccountService accountService;


    @Override
    public Endpoint getMasterEndpointCached(String mha) {
        try {
            if (StringUtils.isEmpty(mha)) {
                return null;
            }
            return cache.get(mha);
        } catch (ExecutionException e) {
            logger.error("getDrcReplicationConfig execution exception", e);
            return null;
        }
    }

    @Override
    public Endpoint getMasterEndpoint(String mha) {
        try {
            if (mha == null) {
                return null;
            }
            if (!defaultConsoleConfig.isCenterRegion()) {
                return this.getMasterEndpointFromDbaApi(mha);
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

    @Override
    public String getUuid(String ip, int port) throws SQLException {
        MachineTbl machineTbl = machineTblDao.queryByIpPort(ip, port);
        if (machineTbl == null) {
            return null;
        }
        String uuid = machineTbl.getUuid();
        if (StringUtils.isEmpty(uuid)) {
            return "";
        }
        return uuid;
    }

    @Override
    public Integer correctUuid(String ip, Integer port, String uuid) throws SQLException {
        MachineTbl machineTbl = machineTblDao.queryByIpPort(ip, port);
        if (machineTbl == null) {
            return 0;
        }
        machineTbl.setUuid(uuid);
        return machineTblDao.update(machineTbl);
    }

    @Override
    public List<Endpoint> getMasterEndpointsInAllAccounts(String mha) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mha, 0);
            if (mhaTblV2 == null) {
                return getMasterEndpointsInAllAccountsFromDbaApi(mha);
            }
            List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaTblV2.getId(), 0);
            if (CollectionUtils.isEmpty(machineTbls)) {
                return getMasterEndpointsInAllAccountsFromDbaApi(mha);
            }
            return getMastersInAllAccounts(mhaTblV2, machineTbls);
        } catch (SQLException e) {
            logger.error("getMasterEndpointsInAllAccounts sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private Endpoint getMasterEndpointFromDbaApi(String mha) {
        // todo hdpan acc
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

    private List<Endpoint> getMasterEndpointsInAllAccountsFromDbaApi(String mha) {
        DbaClusterInfoResponse clusterMembersInfo = dbaApiService.getClusterMembersInfo(mha);
        List<MemberInfo> memberlist = clusterMembersInfo.getData().getMemberlist();
        List<Endpoint> endpoints = new ArrayList<>();
        for (MemberInfo memberInfo : memberlist) {
            if (memberInfo.getRole().toLowerCase().contains("master")) {
                endpoints.add(new MySqlEndpoint(memberInfo.getService_ip(), memberInfo.getDns_port(), monitorTableSourceProvider.getMonitorUserVal(), monitorTableSourceProvider.getMonitorPasswordVal(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(memberInfo.getService_ip(), memberInfo.getDns_port(), monitorTableSourceProvider.getReadUserVal(), monitorTableSourceProvider.getReadPasswordVal(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(memberInfo.getService_ip(), memberInfo.getDns_port(), monitorTableSourceProvider.getWriteUserVal(), monitorTableSourceProvider.getWritePasswordVal(), BooleanEnum.TRUE.isValue()));
                return endpoints;
            }
        }
        return null;
    }

    public Endpoint getMaster(MhaTblV2 mhaTblV2, List<MachineTbl> machineInfo) {
        for (MachineTbl machineTbl : machineInfo) {
            if (machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode())) {
                Account account = accountService.getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_CONSOLE);
                return new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), account.getUser(), account.getPassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }

    public List<Endpoint> getMastersInAllAccounts(MhaTblV2 mhaTblV2, List<MachineTbl> machineInfo) {
        List<Endpoint> endpoints = new ArrayList<>();
        for (MachineTbl machineTbl : machineInfo) {
            if (machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode())) {
                MhaAccounts mhaAccounts = accountService.getMhaAccounts(mhaTblV2);
                endpoints.add(new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaAccounts.getMonitorAcc().getUser(),mhaAccounts.getMonitorAcc().getPassword(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaAccounts.getReadAcc().getUser(), mhaAccounts.getReadAcc().getPassword(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaAccounts.getWriteAcc().getUser(), mhaAccounts.getWriteAcc().getPassword(), BooleanEnum.TRUE.isValue()));
                return endpoints;
            }
        }
        return null;
    }
}
