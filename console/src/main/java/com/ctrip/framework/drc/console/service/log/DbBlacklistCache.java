package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2024/1/25 15:26
 */
@Component
@Lazy
public class DbBlacklistCache implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private SSOService ssoService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private ConflictDbBlackListTblDao conflictDbBlackListTblDao;

    private static final String REFRESH_URL = "http://%s:%s/api/drc/v2/log/conflict/blacklist/refresh";
    private List<BlacklistFilterEntry> blacklist;

    private final LoadingCache<String, Pair<Boolean, Set<String>>> cache = CacheBuilder.newBuilder()
            .maximumSize(100000)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public Pair<Boolean, Set<String>> load(@NotNull String fullName) {
                    return loadCache(fullName);
                }
            });

    @Override
    public void afterPropertiesSet() throws Exception {
        refresh(false);
    }

    public List<AviatorRegexFilter> getDbBlacklistInCache() {
        return blacklist.stream().map(BlacklistFilterEntry::getDbFilter).toList();
    }

    public void refresh(boolean notify) throws Exception {
        logger.info("refresh dbBlacklist, notify: {}", notify);
        blacklist = this.queryBlackList();
        cache.invalidateAll();
        if (notify) {
            List<AppNode> appNodes = ssoService.getAppNodes();
            if (CollectionUtils.isEmpty(appNodes)) {
                logger.warn("refresh dbBlacklist getAppNodes empty");
                return;
            }
            InetAddress localHost = InetAddress.getLocalHost();
            List<String> centerRegionDcs = defaultConsoleConfig.getCenterRegionDcs().stream().map(String::toLowerCase).collect(Collectors.toList());
            for (AppNode appNode : appNodes) {
                if (appNode.getIp().equals(localHost.getHostAddress()) || !appNode.isLegal() || !centerRegionDcs.contains(appNode.getIdc().toLowerCase())) {
                    continue;
                }
                String url = String.format(REFRESH_URL, appNode.getIp(), appNode.getPort());
                try {
                    ApiResult postResult = HttpUtils.post(url, null, ApiResult.class);
                    if (postResult.getStatus().equals(ResultCode.HANDLE_FAIL.getCode())) {
                        logger.warn("notify other machine to refresh fail,ip:port is {}:{}", appNode.getIp(), appNode.getPort());
                    }
                } catch (Exception e) {
                    logger.error("notify other machine to refresh fail,ip:port is {}:{}", appNode.getIp(), appNode.getPort(), e);
                    continue;
                }
                logger.info("notify other machine to refresh success,ip:port is {}:{}", appNode.getIp(), appNode.getPort());
            }
        }
    }

    public boolean isInBlackListWithCache(ConflictRowLog cflLog) {
        try {
            Pair<Boolean, Set<String>> pair = cache.get(cflLog.getDb() + "." + cflLog.getTable());
            if (!pair.getLeft()) {
                return false;
            }
            Set<String> detailBlackList = pair.getRight();
            if (CollectionUtils.isEmpty(detailBlackList)) {
                return true;
            }
            return detailBlackList.contains(cflLog.getConflictDetail());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Pair<Boolean, Set<String>> loadCache(String fullName) {
        Set<String> detailFilter = Sets.newHashSet(); //in case multi black list item (of different type) for same table
        Boolean isInBlackList = false;
        for (BlacklistFilterEntry blacklistFilter : blacklist) {
            AviatorRegexFilter dbfilter = blacklistFilter.getDbFilter();
            if (dbfilter.filter(fullName)) {
                isInBlackList = true;
                detailFilter.addAll(blacklistFilter.getDetailFilter());
            }
        }
        return Pair.of(isInBlackList, detailFilter);
    }

    public List<BlacklistFilterEntry> queryBlackList() throws SQLException {
        if (!defaultConsoleConfig.isCenterRegion()) {
            return new ArrayList<>();
        }
        List<BlacklistFilterEntry> blackList = new ArrayList<>();
        List<ConflictDbBlackListTbl> blackListTbls = conflictDbBlackListTblDao.queryAllExist();
        for (ConflictDbBlackListTbl blackListTbl : blackListTbls) {
            blackList.add(new BlacklistFilterEntry(blackListTbl.getDbFilter(), blackListTbl.getDetailFilter()));
        }
        return blackList;
    }

    public static class BlacklistFilterEntry {
        private AviatorRegexFilter dbFilter;
        private Set<String> detailFilter;

        public BlacklistFilterEntry(String dbFilter, String detailFilter) {
            this.dbFilter = new AviatorRegexFilter(dbFilter);
            this.detailFilter = Optional.ofNullable(detailFilter)
                    .map(filter -> Arrays.stream(filter.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toSet()))
                    .orElse(Sets.newHashSet());
        }

        public AviatorRegexFilter getDbFilter() {
            return dbFilter;
        }

        public Set<String> getDetailFilter() {
            return detailFilter;
        }
    }
}
