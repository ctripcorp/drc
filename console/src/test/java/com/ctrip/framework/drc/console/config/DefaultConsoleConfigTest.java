package com.ctrip.framework.drc.console.config;

import com.ctrip.framework.drc.console.config.meta.DcInfo;
import com.ctrip.framework.drc.console.mock.MockConfig;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-03
 */
public class DefaultConsoleConfigTest {

    private DefaultConsoleConfig config;

    @Before
    public void setUp() {
        config = new DefaultConsoleConfig(new MockConfig());
        config.setDefaultDcInfos("{\"shaoy\":\"http://oy\", \"sharb\":\"http://rb\"}");
        config.setDefaultDbaDcInfos("{\"shanghaiouyang\":\"shaoy\",\"shanghairiban\":\"sharb\",\"jinzhonglubdong\":\"shajz\",\"shanghaijinqiao\":\"shajq\",\"shanghaifuquan\":\"shafq\",\"nantongxinghudadao\":\"ntgxh\",\"shanghaisohodalou\":\"shash\"}");
        config.setDefaultConsoleDcInfos("{\"shaoy\":\"http://consoleoy\", \"sharb\":\"http://consolerb\", \"shant\":\"http://consolent\", \"shast\":\"http://consolent\"}");
        config.setDefaultMhaDalClusterInfos("{\"fat-fx-drc1\":\"bbzdrccameldb_dalcluster,bbzdrcbenchmarkdb_dalcluster\",\"fat-fx-drc2\":\"bbzdrccameldb_dalcluster,bbzdrcbenchmarkdb_dalcluster\"}");
        config.setDefaultBeaconPrefix("http://beancon");
    }

    @Test
    public void testGetDcInfos() {
        Map<String, DcInfo> dcInfos = config.getDcInfos();
        Assert.assertEquals(2, dcInfos.keySet().size());
        Assert.assertEquals(new DcInfo("http://oy"), dcInfos.get("shaoy"));
        Assert.assertEquals(new DcInfo("http://rb"), dcInfos.get("sharb"));
        Assert.assertNull(config.getCMMetaServerAddress("noConfigDc"));
    }

    @Test
    public void testGetBeaconDomain() {
        Assert.assertEquals("http://beancon", config.getBeaconPrefix());
    }

    @Test
    public void testGetDbaDcInfos() {
        Map<String, String> dbaDcInfos = config.getDbaDcInfos();
        Assert.assertEquals(7, dbaDcInfos.keySet().size());
        Assert.assertEquals("shaoy", dbaDcInfos.get("shanghaiouyang"));
        Assert.assertEquals("sharb", dbaDcInfos.get("shanghairiban"));
    }

    @Test
    public void testGetConsoleDcInfos() {
        Map<String, String> consoleDcInfos = config.getConsoleDcInfos();
        Assert.assertEquals(4, consoleDcInfos.size());
        Assert.assertEquals("http://consoleoy", consoleDcInfos.get("shaoy"));
    }

    @Test
    public void testGetMhaDalClusterInfoMapping() {
        Map<String, String> mhaDalClusterInfoMapping = config.getMhaDalClusterInfoMapping();

        Assert.assertTrue(mhaDalClusterInfoMapping.containsKey("fat-fx-drc1"));
        String s = mhaDalClusterInfoMapping.get("fat-fx-drc1");
        String[] split = s.split(",");
        Assert.assertTrue(ArrayUtils.contains(split, "bbzdrcbenchmarkdb_dalcluster"));
        Assert.assertTrue(ArrayUtils.contains(split, "bbzdrccameldb_dalcluster"));
        Assert.assertTrue(mhaDalClusterInfoMapping.containsKey("fat-fx-drc2"));
        s = mhaDalClusterInfoMapping.get("fat-fx-drc2");
        split = s.split(",");
        Assert.assertTrue(ArrayUtils.contains(split, "bbzdrcbenchmarkdb_dalcluster"));
        Assert.assertTrue(ArrayUtils.contains(split, "bbzdrccameldb_dalcluster"));
    }

    @Test
    public void testGetCMMetaServerAddress() {
        Assert.assertEquals("http://oy", config.getCMMetaServerAddress("shaoy"));
        Assert.assertEquals("http://rb", config.getCMMetaServerAddress("sharb"));
    }

    @Test
    public void testGetUcsStrategyIdMap() {
        Map<String, Integer> nosuchcluster = config.getUcsStrategyIdMap("nosuchcluster");
        Assert.assertNotNull(nosuchcluster);
        Assert.assertEquals(0, nosuchcluster.size());
    }

    @Test
    public void testGetUidMap() {
        Map<String, String> nosuchcluster = config.getUidMap("nosuchcluster");
        Assert.assertNotNull(nosuchcluster);
        Assert.assertEquals(0, nosuchcluster.size());
    }
}
