package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MessengerFilterTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.impl.MessengerServiceV2Impl;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/5/30 20:55
 */
public class MessengerServiceV2Test {

    @InjectMocks
    private MessengerServiceV2Impl messengerService;
    @Mock
    private RowsFilterServiceV2 rowsFilterService;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MessengerFilterTblDao messengerFilterTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Before
    public void setUp(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateMessengers() throws SQLException {
        MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
        messengerGroupTbl.setId(0L);
        Mockito.when(messengerGroupTblDao.queryByMhaIdAndMqType(Mockito.anyLong(),Mockito.any(), Mockito.anyInt())).thenReturn(messengerGroupTbl);
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setId(0L);
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(Lists.newArrayList(dbReplicationTbl));
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getDbReplicationFilterMappingTbl());
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.anyString(), Mockito.anyList())).thenReturn(new ArrayList<>());

        MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
        messengerFilterTbl.setProperties(JsonUtils.toJson(new MqConfig()));
        Mockito.when(messengerFilterTblDao.queryById(Mockito.anyLong())).thenReturn(messengerFilterTbl);

        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(0L);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.anyLong())).thenReturn(Lists.newArrayList(messengerTbl));
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(new ResourceTbl());

        List<Messenger> result = messengerService.generateMessengers(0L, MqType.DEFAULT);
        Assert.assertEquals(result.size(), 1);
    }

    private List<DbReplicationFilterMappingTbl> getDbReplicationFilterMappingTbl() {
        DbReplicationFilterMappingTbl tbl = new DbReplicationFilterMappingTbl();
        tbl.setDbReplicationId(0L);
        tbl.setMessengerFilterId(1L);
        return Lists.newArrayList(tbl);
    }



}
