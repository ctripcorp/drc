package com.ctrip.framework.drc.console.service.checker;

import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Set;

/**
 * Created by jixinwang on 2020/11/13
 */
@Component
public class ConflictLogChecker {

    private MonitorTableSourceProvider monitorTableSourceProvider;

    public ConflictLogChecker(MonitorTableSourceProvider monitorTableSourceProvider) {
        this.monitorTableSourceProvider = monitorTableSourceProvider;
    }

    public boolean inBlackList(String dalClusterName) {
        if (StringUtils.isEmpty(dalClusterName)) {
            return false;
        }

        Set<String> blackList = monitorTableSourceProvider.getConflictBlackList();
        if (CollectionUtils.isEmpty(blackList)) {
            return false;
        }

        return blackList.contains(dalClusterName);
    }
}
