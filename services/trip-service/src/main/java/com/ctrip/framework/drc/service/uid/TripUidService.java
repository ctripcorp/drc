package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.AccountUidRoute;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
import com.ctrip.soa.platform.accountregionroute.v1.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status.Illegal;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class TripUidService implements UidService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int RETRY_TIME = 2;

    @Override
    public RowsFilterResult.Status filterUid(UserContext userContext) throws Exception {
        RowsFilterResult.Status result = new RetryTask<>(new UidFilterTask(userContext), RETRY_TIME).call();

        if (result == null) {
            throw new Exception("[RowsFilter] do UidFilterTask error, null result");
        }
        return result;
    }

    @Override
    public RowsFilterResult.Status filterUdl(UserContext uidContext) throws Exception {
        RowsFilterResult.Status result = new UdlFilterTask(uidContext).call();

        if (result == null) {
            throw new Exception("[RowsFilter] do UdlFilterTask error, null result");
        }
        return result;
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private abstract class UserFilterTask implements NamedCallable<RowsFilterResult.Status> {

        private String userAttr;

        private Set<String> locations;

        public UserFilterTask(UserContext userContext) {
            this.userAttr = userContext.getUserAttr();
            this.locations = userContext.getLocations();
        }

        @Override
        public RowsFilterResult.Status call() throws Exception {
            try {
                if (locations == null || locations.isEmpty()) {
                    return RowsFilterResult.Status.from(false);
                }
                Region region = regionFor(userAttr);
                return RowsFilterResult.Status.from(locations.contains(region.name().toUpperCase()));
            } catch (Exception e) {
                return Illegal;
            }
        }

        protected abstract Region regionFor(String attr);

        @Override
        public void afterException(Throwable t) {
            logger.error("[RowsFilter] call sdk with userAttr({}) error", userAttr, t);
        }

        @Override
        public void afterSuccess(int retryTime) {
        }
    }

    private class UidFilterTask extends UserFilterTask {

        public UidFilterTask(UserContext uidContext) {
            super(uidContext);
        }

        @Override
        protected Region regionFor(String attr) {
            return AccountUidRoute.regionForUid(attr);
        }
    }

    private class UdlFilterTask extends UserFilterTask {

        public UdlFilterTask(UserContext uidContext) {
            super(uidContext);
        }

        @Override
        protected Region regionFor(String attr) {
            //TODO
            return AccountUidRoute.regionForUid(attr);
        }
    }
}
