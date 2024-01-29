package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by dengquanliang
 * 2024/1/25 15:26
 */
@Component
public class DbBlacklistCache {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictLogService conflictLogService;

    private final static String KEY = "key";
    private final LoadingCache<String, List<AviatorRegexFilter>> cache = CacheBuilder.newBuilder()
            .maximumSize(1)
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public List<AviatorRegexFilter> load(@NotNull String o) {
                    return conflictLogService.queryBlackList();
                }
            });

    public List<AviatorRegexFilter> getDbBlacklistInCache() {
        try {
            return cache.get(KEY);
        } catch (ExecutionException e) {
            logger.error("getDbBlacklistInCache error: {}", e);
            return new ArrayList<>();
        }
    }

    public void refresh() {
        cache.refresh(KEY);
    }
}
