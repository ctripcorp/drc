package com.ctrip.framework.drc.replicator.container.config;

import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.BlackTableNameFilter;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2021/8/3
 */
public class TableFilterConfigurationTest extends MockTest {

    private static final String KEY1 = "regitryKey";

    private static final String KEY_VALUE_0 = "qmq_msg_queue";
    private static final String KEY_VALUE_1 = "qmq_msg_queue1";
    private static final String KEY_VALUE_2 = "qmq_msg_queue2";

    @InjectMocks
    private TableFilterConfiguration tableFilterConfiguration = new TableFilterConfiguration();

    @Mock
    private InboundMonitorReport inboundMonitorReport;

    private BlackTableNameFilter filterWithConfiguration = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());

    private BlackTableNameFilter filterWithDefaultConfiguration = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());

    @Before
    public void setUp() throws Exception {
        super.initMocks();
    }

    @Test
    public void testTableFilterConfiguration() {
        // test custom config
        tableFilterConfiguration.register(KEY1, filterWithConfiguration);
        Set<String> filteredTables = filterWithConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 1);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_2));
        tableFilterConfiguration.unregister(KEY1);

        // test default config
        String newKey = KEY1 + 123456;
        tableFilterConfiguration.register(newKey, filterWithDefaultConfiguration);
        filteredTables = filterWithDefaultConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 2);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_0));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_1));
        tableFilterConfiguration.unregister(newKey);

        // test update
        BlackTableNameFilter localFilterWithConfiguration = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());
        tableFilterConfiguration.register(KEY1, localFilterWithConfiguration);  // KEY_VALUE_2
        tableFilterConfiguration.register(newKey, filterWithDefaultConfiguration);  // KEY_VALUE_0, KEY_VALUE_1

        tableFilterConfiguration.onChange(TableFilterConfiguration.FILTER_TABLES, KEY_VALUE_0 + "," + KEY_VALUE_1, KEY_VALUE_0 + "," + KEY_VALUE_1 +  "," + KEY_VALUE_2);

        filteredTables = filterWithDefaultConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 3);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_0));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_1));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_2));

        filteredTables = localFilterWithConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 1);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_2));

        String newValue = "1qaz2wsx";
        tableFilterConfiguration.onChange(TableFilterConfiguration.FILTER_TABLES + "." + KEY1, KEY_VALUE_0 + "," + KEY_VALUE_1, newValue);

        filteredTables = filterWithDefaultConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 3);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_0));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_1));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_2));

        filteredTables = localFilterWithConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 1);
        Assert.assertTrue(filteredTables.contains(newValue));
    }

    @Test
    public void testBlankFilter() {
        BlackTableNameFilter localFilterWithConfiguration = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());
        tableFilterConfiguration.register("blank", localFilterWithConfiguration);  // empty string
        Set<String> filteredTables = localFilterWithConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 0);

        localFilterWithConfiguration = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());
        tableFilterConfiguration.register("blank_not_config", localFilterWithConfiguration);  // empty string
        filteredTables = localFilterWithConfiguration.getEXCLUDED_TABLE();
        Assert.assertEquals(filteredTables.size(), 2);
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_0));
        Assert.assertTrue(filteredTables.contains(KEY_VALUE_1));
    }
}