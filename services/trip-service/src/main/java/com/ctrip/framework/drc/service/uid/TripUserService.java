package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.AccountUidRoute;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.common.filter.row.Region;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;
import com.ctrip.framework.drc.core.server.common.filter.service.UserService;
import com.ctrip.framework.ucs.client.ShardingKeyValue;
import com.ctrip.framework.ucs.client.api.RequestContext;
import com.ctrip.framework.ucs.client.api.UcsClient;
import com.ctrip.framework.ucs.client.api.UcsClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status.Illegal;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class TripUserService implements UserService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int RETRY_TIME = 2;

    private static UcsClient ucsClient = UcsClientFactory.getInstance().getUcsClient();

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
        public RowsFilterResult.Status call() {
            try {
                if (locations == null || locations.isEmpty()) {
                    return RowsFilterResult.Status.from(false);
                }
                Region region = regionFor(userAttr);
                logger.info(
                        "[UserService] for {},attr:{},region:{},locations",
                        this.getClass().getSimpleName(), userAttr, region.name()
                );
                if (region == null) {
                    return Illegal;
                }
                return RowsFilterResult.Status.from(locations.contains(region.name().toUpperCase()));
            } catch (Exception e) {
                logger.error("regionFor {} error", userAttr, e);
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
            com.ctrip.soa.platform.accountregionroute.v1.Region region =  AccountUidRoute.regionForUid(attr);
            return Region.nameFor(region.name().toUpperCase());
        }
    }

    private class UdlFilterTask extends UserFilterTask {

        private int drcStrategyId;

        public UdlFilterTask(UserContext userContext) {
            super(userContext);
            this.drcStrategyId = userContext.getDrcStrategyId();
        }

        @Override
        protected Region regionFor(String attr) {
            if (StringUtils.isBlank(attr)) {
                return Region.SHA;
            }
            ShardingKeyValue udlKey = ShardingKeyValue.ofUdl(attr);
            RequestContext ctx = ucsClient.buildRequestContext(drcStrategyId, udlKey);
            Optional<String> region = ctx.getRequestRegion();
            if (region.isEmpty()) {
                logger.info("[UdlFilter] ucsStrategyId:{},noRegion find for udl:{}",drcStrategyId,udlKey);
                return Region.SHA;
            }
            logger.info("[UdlFilter] ucsStrategyId:{},region:{} find for udl:{}",drcStrategyId,region.get().toUpperCase(),udlKey);
            return Region.nameFor(region.get().toUpperCase());
        }
    }
}
