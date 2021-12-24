package com.ctrip.framework.drc.console.service.checker;

import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Set;

/**
 * Created by jixinwang on 2020/10/23
 */
@Component
public class UserAccessChecker {

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    public boolean isAllowed(String userName) {
        if (StringUtils.isEmpty(userName)) {
            return false;
        }

        Set<String> allowedUsers = monitorTableSourceProvider.getAdminUser();
        if (CollectionUtils.isEmpty(allowedUsers)) {
            return false;
        }

        return allowedUsers.contains(userName);
    }
}
