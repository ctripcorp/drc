package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.TableConfig;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @Author limingdong
 * @create 2021/7/19
 */
public class SyncTableConfigTaskTest {

    @InjectMocks
    private SyncTableConfigTask syncTableConfigTask;

    @Mock
    private DalServiceImpl dalService;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MetaGenerator metaService;

    private List<ReplicatorGroupTbl> replicatorGroupTbls = Lists.newArrayList();

    private Map<String, Map<String, List<String>>> mhaName2Dbs = Maps.newHashMap();

    private MhaTbl mhaTbl = new MhaTbl();

    public static final long MHA_ID = 20l;

    public static final String MHA_NAME = "testMhaName";

    public static final String DAL_CLUSTER_NAME = "testDalClusterName";

    public static final String DB_NAME_1 = "testDBName1";

    public static final String DB_NAME_2 = "testDBName2";

    public static final String TABLE_NAME_1 = "testTableName1";

    public List<String> dbNameLists = Lists.newArrayList();

    public List<TableConfig> tableConfigLists = Lists.newArrayList();

    private TableConfig tableConfig = new TableConfig();

    private ReplicatorGroupTbl groupTbl = new ReplicatorGroupTbl();

    private AutoCloseable autoCloseable;

    @Before
    public void setUp() throws Exception {
        autoCloseable = MockitoAnnotations.openMocks(this);

        groupTbl.setExcludedTables("");
        groupTbl.setDeleted(0);
        groupTbl.setId(1l);
        groupTbl.setMhaId(MHA_ID);
        long time = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(time);
        groupTbl.setCreateTime(timestamp);
        groupTbl.setDatachangeLasttime(timestamp);
        replicatorGroupTbls.add(groupTbl);

        mhaTbl.setMhaName(MHA_NAME);
        mhaTbl.setId(MHA_ID);
        mhaTbl.setId(1l);
        mhaTbl.setMhaGroupId(3l);
        mhaTbl.setDeleted(0);
        mhaTbl.setCreateTime(timestamp);
        mhaTbl.setDatachangeLasttime(timestamp);

        Map<String, List<String>> clusterName2Dbs = Maps.newHashMap();
        mhaName2Dbs.put(MHA_NAME, clusterName2Dbs);
        dbNameLists.add(DB_NAME_1);
        dbNameLists.add(DB_NAME_2);
        clusterName2Dbs.put(DAL_CLUSTER_NAME, dbNameLists);

        tableConfig.setIgnoreReplication(true);
        tableConfig.setTableName(TABLE_NAME_1);
        tableConfig.setUcsShardColumn("");
        tableConfigLists.add(tableConfig);

        Mockito.when(metaService.getReplicatorGroupTbls()).thenReturn(replicatorGroupTbls);
        Mockito.when(dalService.getDbNames(Mockito.anyList(), Mockito.any())).thenReturn(mhaName2Dbs);
        Mockito.when(metaInfoService.getMha(MHA_ID)).thenReturn(mhaTbl);
        Mockito.when(dalService.getTableConfigs(DAL_CLUSTER_NAME)).thenReturn(tableConfigLists);
        Mockito.when(monitorTableSourceProvider.getSyncTableConfigSwitch()).thenReturn(SWITCH_STATUS_ON);

        syncTableConfigTask.initialize();
        syncTableConfigTask.isleader();
    }

    @After
    public void tearDown() throws Exception {
        autoCloseable.close();
    }

    @Test
    public void updateExcludedTable() {
        syncTableConfigTask.scheduledTask();
        Assert.assertEquals(groupTbl.getExcludedTables(), "testDBName1.testTableName1,testDBName2.testTableName1");
    }
}