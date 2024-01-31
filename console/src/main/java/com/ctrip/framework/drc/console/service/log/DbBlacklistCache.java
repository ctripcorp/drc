package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/25 15:26
 */
@Component
public class DbBlacklistCache implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictLogService conflictLogService;

    private static List<AviatorRegexFilter> blacklist;

    @Override
    public void afterPropertiesSet() throws Exception {
        refresh();
    }

    public List<AviatorRegexFilter> getDbBlacklistInCache() {
        return blacklist;
    }

    public void refresh() throws SQLException {
        try {
            blacklist = conflictLogService.queryBlackList();
        } catch (SQLException e) {
            logger.error("refresh blacklist error", e);
        }
    }

}
