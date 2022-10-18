package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class DataMediaConfigTest {

    /**
     {"rowsFilters":[{"mode":"trip_udl","tables":"table1","parameters":{"columns":["columnA","columnB","cloumnC"],"context":"regre1"},"configs":{"parameterList":[{"columns":["columnA","columnB","cloumnC"],"context":"regre1"}]}},{"mode":"aviator_regex","tables":"table2","parameters":{"columns":["cloumnA"],"context":"regre2"},"configs":{"parameters":[{"columns":["cloumnA"],"context":"regre2"}]}}],"talbePairs":[{"source":"sourceTableName1","target":"targetTableName1"},{"source":"sourceTableName2","target":"targetTableName2"}]}
     */

    public static final String MEDIA_CONFIG = "";

    private DataMediaConfig rowsFilterConfigs;

    @Before
    public void setUp() throws Exception {
        rowsFilterConfigs = DataMediaConfig.from("ut_test", MEDIA_CONFIG);
    }

    @Test
    public void getRowsFilterRule() {
        List<RowsFilterConfig> rowsFilters = rowsFilterConfigs.getRowsFilters();
        Assert.assertEquals(2, rowsFilters.size());
        Assert.assertTrue(rowsFilterConfigs.shouldFilterRows());

        Optional<RowsFilterRule> optional = rowsFilterConfigs.getRowsFilterRule("table1");
        Assert.assertTrue(optional.isPresent());
        Assert.assertNotNull(optional.get());

        // cache
        optional = rowsFilterConfigs.getRowsFilterRule("table1");
        Assert.assertTrue(optional.isPresent());
        Assert.assertNotNull(optional.get());

        optional = rowsFilterConfigs.getRowsFilterRule("table1_wrong");
        Assert.assertFalse(optional.isPresent());
    }

    @Test
    public void getBlank() throws Exception {
        DataMediaConfig blank = DataMediaConfig.from("ut_test", "");
        List<RowsFilterConfig> rowsFilters = blank.getRowsFilters();
        Assert.assertNull(rowsFilters);
        Assert.assertFalse(blank.shouldFilterRows());

        Optional<RowsFilterRule> optional = blank.getRowsFilterRule("table1");
        Assert.assertFalse(optional.isPresent());

        optional = blank.getRowsFilterRule("table1_wrong");
        Assert.assertFalse(optional.isPresent());
    }
}
