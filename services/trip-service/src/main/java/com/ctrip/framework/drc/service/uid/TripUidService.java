package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.AccountUidRoute;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
import com.ctrip.soa.platform.accountregionroute.v1.Region;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class TripUidService implements UidService {

    @Override
    public boolean filterUid(String uid, Set<String> locations) throws Exception {
        if (locations == null || locations.isEmpty()) {
            return false;
        }
        Region region = AccountUidRoute.regionForUid(uid);
        return locations.contains(region.name().toUpperCase());
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
