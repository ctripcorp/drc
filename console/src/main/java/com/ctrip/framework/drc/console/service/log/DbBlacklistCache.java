package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2024/1/25 15:26
 */
@Component
public class DbBlacklistCache implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictLogService conflictLogService;
    @Autowired
    private SSOService ssoService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    private static final String REFRESH_URL = "http://%s:%s/api/drc/v2/log/blacklist/refresh";
    private static List<AviatorRegexFilter> blacklist;

    @Override
    public void afterPropertiesSet() throws Exception {
        refresh(false);
    }

    public List<AviatorRegexFilter> getDbBlacklistInCache() {
        return blacklist;
    }


    public void refresh(boolean notify) throws Exception {
        blacklist = conflictLogService.queryBlackList();
        if (notify) {
            List<AppNode> appNodes = ssoService.getAppNodes();
            InetAddress localHost = InetAddress.getLocalHost();
            Set<String> centerRegionDcs = defaultConsoleConfig.getCenterRegionDcs();
            for (AppNode appNode : appNodes) {
                if (appNode.getIp().equals(localHost.getHostAddress()) || !appNode.isLegal() || !centerRegionDcs.contains(appNode.getIdc())) {
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


}
