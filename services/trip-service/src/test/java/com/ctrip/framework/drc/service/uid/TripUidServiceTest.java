package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.UDL;
import com.ctrip.framework.drc.core.server.common.filter.row.UidContext;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
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

    private UidService uidService = new TripUidService();

    private Set<String> locations = Sets.newHashSet();

    @Before
    public void setUp() {
        locations.add(Region.SIN.name());
    }

    @Test
    public void filterUid() throws Exception {
        UidContext uidContext = new UidContext();
        uidContext.setUid("test_uid");
        uidContext.setLocations(locations);
        uidContext.setIllegalArgument(false);
        Assert.assertTrue(uidService.filterUid(uidContext));
    }
}
