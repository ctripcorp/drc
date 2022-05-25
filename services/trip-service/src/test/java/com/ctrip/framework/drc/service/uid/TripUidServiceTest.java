package com.ctrip.framework.drc.service.uid;

import com.ctrip.basebiz.tripaccount.region.route.sdk.UDL;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
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
        locations.add(UDL.CN.name());
    }

    @Test
    public void filterUid() throws Exception {
        Assert.assertTrue(uidService.filterUid("test_uid", locations, false));
    }
}
