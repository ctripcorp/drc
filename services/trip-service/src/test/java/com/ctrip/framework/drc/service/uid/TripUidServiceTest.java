package com.ctrip.framework.drc.service.uid;

import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;
import com.ctrip.framework.drc.core.server.common.filter.service.UserService;
import com.ctrip.soa.platform.accountregionroute.v1.Region;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class TripUidServiceTest {

    private UserService uidService = new TripUserService();

    private Set<String> locations = Sets.newHashSet();

    @Before
    public void setUp() {
        locations.add(Region.SIN.name());
    }

    @Test
    public void filterUid() throws Exception {
        UserContext userContext = new UserContext();
        userContext.setUserAttr("test_uid");
        userContext.setLocations(locations);
        userContext.setIllegalArgument(false);
        Assert.assertFalse(uidService.filterUid(userContext).noRowFiltered());
    }

    @Test
    public void filterUdl() throws Exception {
        UserContext userContext = new UserContext();
        userContext.setUserAttr("SG");
        userContext.setLocations(locations);
        userContext.setIllegalArgument(false);
        userContext.setDrcStrategyId(1);
        Assert.assertFalse(uidService.filterUdl(userContext).noRowFiltered());
    }
}
