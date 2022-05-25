package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.AccountUidRoute;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.common.filter.row.UidContext;
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
    public boolean filterUid(UidContext uidContext) throws Exception {
        Boolean result = new RetryTask<>(new UidFilterTask(uidContext), RETRY_TIME).call();

        if (result == null) {
            throw new Exception("[RowsFilter] do UidFilterTask error, null result");
        }
        return result;
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private class UidFilterTask implements NamedCallable<Boolean> {

        private String uid;

        private Set<String> locations;

        private boolean illegalArgument;

        public UidFilterTask(UidContext uidContext) {
            this.uid = uidContext.getUid();
            this.locations = uidContext.getLocations();
            this.illegalArgument = uidContext.getIllegalArgument();
        }

        @Override
        public Boolean call() throws Exception {
            try {
                if (locations == null || locations.isEmpty()) {
                    return false;
                }
                Region region = AccountUidRoute.regionForUid(uid);
                return locations.contains(region.name().toUpperCase());
            } catch (IllegalArgumentException e) {
                return illegalArgument;
            }
        }

        @Override
        public void afterException(Throwable t) {
            logger.error("[RowsFilter] call sdk with uid({}) error", uid, t);
        }
    }
}
