package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.AccountUidRoute;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
import com.ctrip.soa.platform.accountregionroute.v1.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class TripUidService implements UidService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int RETRY_TIME = 2;

    @Override
    public boolean filterUid(String uid, Set<String> locations) throws Exception {
        if (locations == null || locations.isEmpty()) {
            return false;
        }
        Region region = new RetryTask<>(new RegionForUidTask(uid), RETRY_TIME).call();
        if (region == null) {
            throw new Exception("[RowsFilter] call sdk error, null region");
        }
        return locations.contains(region.name().toUpperCase());
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private class RegionForUidTask implements NamedCallable<Region> {

        private String uid;

        public RegionForUidTask(String uid) {
            this.uid = uid;
        }

        @Override
        public Region call() throws Exception {
//            AccountUidRoute.checkUidInSG(uid);
            return AccountUidRoute.regionForUid(uid);
        }

        @Override
        public void afterException(Throwable t) {
            logger.error("[RowsFilter] call sdk for region with uid({}) failed", uid, t);
        }
    }
}
