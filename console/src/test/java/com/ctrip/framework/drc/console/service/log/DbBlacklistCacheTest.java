package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/25 15:44
 */
public class DbBlacklistCacheTest {

    @InjectMocks
    private DbBlacklistCache dbBlacklistCache;
    @Mock
    private ConflictDbBlackListTblDao conflictDbBlackListTblDao;

    @Mock
    private SSOService ssoService;
    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetDbBlacklistInCache() throws Exception {
        Mockito.when(defaultConsoleConfig.getCenterRegionDcs()).thenReturn(Lists.newArrayList("shaxy", "sharb"));
        Mockito.when(defaultConsoleConfig.isCenterRegion()).thenReturn(true);

        AppNode appNode = new AppNode();
        appNode.setIp("ip1");
        appNode.setPort(8080);
        appNode.setIdc("shaxy");
        AppNode appNode1 = new AppNode();
        appNode1.setIp("localhost");
        appNode1.setPort(8080);
        appNode1.setIdc("sha");
        ArrayList<AppNode> appNodes = Lists.newArrayList(appNode, appNode1);
        Mockito.when(ssoService.getAppNodes()).thenReturn(appNodes);

        Mockito.when(conflictDbBlackListTblDao.queryAllExist()).thenReturn(Lists.newArrayList(getConflictDbBlackListTbl("test")));
        dbBlacklistCache.afterPropertiesSet();

        List<AviatorRegexFilter> res = dbBlacklistCache.getDbBlacklistInCache();
//        System.out.println(res);
        Assert.assertEquals(res.get(0).toString(), new AviatorRegexFilter("test").toString());

        Mockito.when(conflictDbBlackListTblDao.queryAllExist()).thenReturn(Lists.newArrayList(getConflictDbBlackListTbl("test1")));
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any())).thenReturn(ApiResult.getSuccessInstance(true));
            dbBlacklistCache.refresh(true);
            res = dbBlacklistCache.getDbBlacklistInCache();

            Assert.assertEquals(res.get(0).toString(), new AviatorRegexFilter("test1").toString());
            Mockito.when(ssoService.getAppNodes()).thenReturn(new ArrayList<>());
            dbBlacklistCache.refresh(true);

            theMock.when(() -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any())).thenReturn(ApiResult.getFailInstance(false));
            Mockito.when(ssoService.getAppNodes()).thenReturn(appNodes);
            dbBlacklistCache.refresh(true);

            theMock.when(() -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any())).thenThrow(new ConsoleException());
            dbBlacklistCache.refresh(true);
        }


    }

    private static ConflictDbBlackListTbl getConflictDbBlackListTbl(String dbFilter) {
        ConflictDbBlackListTbl conflictDbBlackListTbl = new ConflictDbBlackListTbl();
        conflictDbBlackListTbl.setDbFilter(dbFilter);
        return conflictDbBlackListTbl;
    }
}
