package com.ctrip.framework.drc.console.service.v2.security.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.security.MetaAccountService;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName MetaAccountServiceImpl
 * @Author haodongPan
 * @Date 2024/6/28 11:33
 * @Version: $
 */
@Service
public class MetaAccountServiceImpl implements MetaAccountService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private CentralService centralService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    

    private LoadingCache<String,MhaAccounts> mhaAccountsCache = CacheBuilder.newBuilder()
            .maximumSize(3000)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public MhaAccounts load(@NotNull String mhaName) {
                    return loadCache(mhaName);
                }
            });
    
    

    @Override
    public MhaAccounts getMhaAccounts(String mhaName) {
        try {
            if (consoleConfig.getAccountFromMetaSwitch()) {
                return mhaAccountsCache.get(mhaName);
            } else {
                return new MhaAccounts(
                        mhaName,
                        new Account(monitorTableSourceProvider.getMonitorUserVal(),monitorTableSourceProvider.getMonitorPasswordVal()),
                        new Account(monitorTableSourceProvider.getReadUserVal(),monitorTableSourceProvider.getReadPasswordVal()),
                        new Account(monitorTableSourceProvider.getWriteUserVal(),monitorTableSourceProvider.getWritePasswordVal())
                );
            }
            
        } catch (ExecutionException e) {
            logger.error("mha:{},getMhaAccounts error",mhaName,e);
            throw new RuntimeException(e);
        }
    }


    protected MhaAccounts loadCache(String mhaName) {
        Drc drc = metaProviderV2.getDrc();
        Map<String, Dc> dcs = drc.getDcs();
        for (Entry<String, Dc> dcEntry : dcs.entrySet()) {
            Dc dc = dcEntry.getValue();
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            Entry<String, DbCluster> mhaDbCluster = dbClusters.entrySet().stream()
                    .filter(dbCluster -> dbCluster.getValue().getMhaName().equals(mhaName)).findFirst().orElse(null);
            if (mhaDbCluster != null) {
                Dbs dbs = mhaDbCluster.getValue().getDbs();
                return new MhaAccounts(
                        mhaName,
                        new Account(dbs.getMonitorUser(),dbs.getMonitorPassword()),
                        new Account(dbs.getReadUser(),dbs.getReadPassword()),
                        new Account(dbs.getWriteUser(),dbs.getWritePassword())
                );
            }
        }
        // no mha cluster found in meta xml,get AccountInfo from metaDb
        try {
            return centralService.getMhaAccounts(mhaName);
        } catch (Exception e) {
            logger.error("mha:{},getMhaAccounts error",mhaName,e);
            return null;
        }
    }
}
