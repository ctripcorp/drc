package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.AllTests.ROW_FILTER_PROPERTIES;
import static com.ctrip.framework.drc.core.meta.DataMediaConfig.from;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class AbstractRowsFilterRuleTest extends AbstractEventTest {

    private DataMediaConfig dataMediaConfig;

    private AbstractRowsFilterRule rowsFilterRule;

    private TableMapLogEvent tableMapLogEvent;

    private TableMapLogEvent drcTableMapLogEvent;

    private WriteRowsEvent writeRowsEvent;

    private List<List<Object>> result = Lists.newArrayList();


    @Before
    public void setUp() throws Exception {
        dataMediaConfig = from("registryKey", String.format(ROW_FILTER_PROPERTIES, RowsFilterType.TripUid.getName()));
        rowsFilterRule = new TestRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));

        result.add(Lists.newArrayList("1", "2"));
        ByteBuf tByteBuf = tableMapEvent();
        tableMapLogEvent = new TableMapLogEvent().read(tByteBuf);
        ByteBuf wByteBuf = writeRowsEvent();
        writeRowsEvent = new WriteRowsEvent().read(wByteBuf);
        drcTableMapLogEvent = drcTableMapEvent();

        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
        writeRowsEvent.load(columns);
    }

    @Test
    public void filterRow() throws Exception {
        RowsFilterResult<List<List<Object>>> res = rowsFilterRule.filterRows(writeRowsEvent, drcTableMapLogEvent);
        Assert.assertFalse(res.isNoRowFiltered());
        Assert.assertEquals(res.getRes(), result);
    }

    class TestRowsFilterRule extends AbstractRowsFilterRule {

        public TestRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
            super(rowsFilterConfig);
        }

        @Override
        protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) {
            Assert.assertEquals(3, values.size());
            Assert.assertEquals(2, indices.size());  // id、one
            Assert.assertEquals(0, indices.get("id").intValue());  // id in index 0
            Assert.assertEquals(1, indices.get("one").intValue());  // one in index 1
            return result;
        }
    }
}