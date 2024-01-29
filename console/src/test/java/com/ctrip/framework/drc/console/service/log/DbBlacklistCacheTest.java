package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/25 15:44
 */
public class DbBlacklistCacheTest {

    @InjectMocks
    private DbBlacklistCache dbBlacklistCache;
    @Mock
    private ConflictLogService conflictLogService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetDbBlacklistInCache() {
        Mockito.when(conflictLogService.queryBlackList()).thenReturn(Lists.newArrayList(new AviatorRegexFilter("test")));
        List<AviatorRegexFilter> res = dbBlacklistCache.getDbBlacklistInCache();
//        System.out.println(res);
        Assert.assertEquals(res.get(0).toString(), new AviatorRegexFilter("test").toString());

        Mockito.when(conflictLogService.queryBlackList()).thenReturn(Lists.newArrayList(new AviatorRegexFilter("test1")));
        dbBlacklistCache.refresh();
        res = dbBlacklistCache.getDbBlacklistInCache();
//        System.out.println(res);
        Assert.assertEquals(res.get(0).toString(), new AviatorRegexFilter("test1").toString());

    }
}
