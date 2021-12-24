package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MhaServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Maps;
import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doReturn;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-10
 */
public class PeriodicalUpdateMhasTaskTest {

    @InjectMocks
    private PeriodicalUpdateMhasTask periodicalUpdateMhasTask;

    @Mock
    private MhaServiceImpl mhaService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    private DalUtils dalUtils = DalUtils.getInstance();

    private List<Map<String, String>> allMhaMapsFromDba1;

    private List<Map<String, String>> allMhaMapsFromDba2;

    private List<Map<String, String>> allMhaMapsFromDba3;

    private List<Map<String, String>> allMhaMapsFromDba4;

    private List<Map<String, String>> allMhaMapsFromDba5;

    @Before
    public void setUp() {
        allMhaMapsFromDba1 = new ArrayList<>() {{
            add(new HashMap<>() {{
                put("cluster", "mhaName1");
                put("zoneId", "上海欧阳IDC(电信)");
            }});
            add(new HashMap<>() {{
                put("cluster", "mhaName2");
                put("zoneId", "上海日阪IDC(联通)");
            }});
        }};
        allMhaMapsFromDba2 = new ArrayList<>() {{
            add(new HashMap<>() {{
                put("cluster", "mhaName1");
                put("zoneId", "上海欧阳IDC(电信)");
            }});
            add(new HashMap<>() {{
                put("cluster", "mhaName3");
                put("zoneId", "上海金钟路B栋");
            }});
        }};
        allMhaMapsFromDba3 = new ArrayList<>() {{
            add(new HashMap<>() {{
                put("cluster", "mhaName1");
                put("zoneId", "上海欧阳IDC(电信)");
            }});
            add(new HashMap<>() {{
                put("cluster", "mhaName2");
                put("zoneId", "上海日阪IDC(联通)");
            }});
            add(new HashMap<>() {{
                put("cluster", "mhaName3");
                put("zoneId", "上海金钟路B栋");
            }});
        }};
        allMhaMapsFromDba4 = new ArrayList<>();
        allMhaMapsFromDba5 = new ArrayList<>() {{
            add(new HashMap<>() {{
                put("cluster", "mhaName4");
                put("zoneId", "unknownzoneId");
            }});
        }};

        MockitoAnnotations.openMocks(this);
        doReturn("on").when(monitorTableSourceProvider).getUpdateClusterTblSwitch();
        doReturn(getDbaDcInfoMapping("{\"上海欧阳IDC(电信)\":\"shaoy\", \"上海日阪IDC(联通)\":\"sharb\", \"上海金钟路B栋\":\"shajz\", \"上海金桥IDC(联通)\":\"shajq\", \"上海福泉路\":\"shafq\", \"南通星湖大道\":\"ntgxh\", \"上海SOHO大楼\":\"shash\"}")).when(defaultConsoleConfig).getDbaDcInfos();
        periodicalUpdateMhasTask.setInitialDelay(0);
        periodicalUpdateMhasTask.setPeriod(100);
        periodicalUpdateMhasTask.setTimeUnit(TimeUnit.MILLISECONDS);
    }

    @Test
    public void testGetNewMhas() throws InterruptedException, SQLException {
        AllTests.truncateAllMetaDb();
        doReturn(allMhaMapsFromDba4).when(mhaService).getAllClusterNames();
        periodicalUpdateMhasTask.start();

        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryAll();
        int initSize = mhaTbls.size();
        Assert.assertTrue(0 == initSize || 4 == initSize);

        doReturn(allMhaMapsFromDba1).when(mhaService).getAllClusterNames();
        Thread.sleep(500);
        Assert.assertEquals(2, dalUtils.getDcTblDao().queryAll().size());
        Assert.assertEquals(initSize + 2, dalUtils.getMhaTblDao().queryAll().size());

        doReturn(allMhaMapsFromDba2).when(mhaService).getAllClusterNames();
        Thread.sleep(500);
        Assert.assertEquals(3, dalUtils.getDcTblDao().queryAll().size());
        Assert.assertEquals(initSize + 3, dalUtils.getMhaTblDao().queryAll().size());

        doReturn(allMhaMapsFromDba3).when(mhaService).getAllClusterNames();
        Thread.sleep(500);
        Assert.assertEquals(3, dalUtils.getDcTblDao().queryAll().size());
        Assert.assertEquals(initSize + 3, dalUtils.getMhaTblDao().queryAll().size());

        doReturn(allMhaMapsFromDba4).when(mhaService).getAllClusterNames();
        Thread.sleep(500);
        Assert.assertEquals(3, dalUtils.getDcTblDao().queryAll().size());
        Assert.assertEquals(initSize + 3, dalUtils.getMhaTblDao().queryAll().size());

        doReturn(allMhaMapsFromDba5).when(mhaService).getAllClusterNames();
        Thread.sleep(500);
        Set<String> dc = Sets.newHashSet();
        dalUtils.getDcTblDao().queryAll().forEach(d -> dc.add(d.getDcName()));
        Assert.assertEquals(4, dc.size());
        Assert.assertTrue(dc.contains("unknownzoneId"));
        Assert.assertEquals(initSize + 4, dalUtils.getMhaTblDao().queryAll().size());
    }

    @After
    public void tearDown() {

    }

    public static Map<String, String> getDbaDcInfoMapping(String dbaDcInfoStr) {
        Map<String, String> dbaDcInfos = JsonCodec.INSTANCE.decode(dbaDcInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : dbaDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }
        return result;
    }
}