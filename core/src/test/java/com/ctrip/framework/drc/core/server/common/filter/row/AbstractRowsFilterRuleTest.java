package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class AbstractRowsFilterRuleTest extends AbstractEventTest {

    private AbstractRowsFilterRule rowsFilterRule = new TestRowsFilterRule("{\"drc1.insert1\":[\"id\"]}");

    private TableMapLogEvent tableMapLogEvent;

    private TableMapLogEvent drcTableMapLogEvent;

    private WriteRowsEvent writeRowsEvent;

    private List<List<Object>> result = Lists.newArrayList();


    @Before
    public void setUp() throws IOException {
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
    public void filterRow() {
        RowsFilterResult<List<List<Object>>> res = rowsFilterRule.filterRow(writeRowsEvent, drcTableMapLogEvent);
        Assert.assertFalse(res.isNoRowFiltered());
        Assert.assertEquals(res.getRes(), result);
    }

    class TestRowsFilterRule extends AbstractRowsFilterRule {

        public TestRowsFilterRule(String context) {
            super(context);
        }

        @Override
        protected List<List<Object>> doRowsFilter(List<List<Object>> values, List<Integer> indices) {
            Assert.assertEquals(3, values.size());
            Assert.assertEquals(1, indices.size());
            Assert.assertEquals(0, indices.get(0).intValue());  // id in index 0
            return result;
        }
    }
}