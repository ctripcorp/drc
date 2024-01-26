package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * try udl first, then uid second
 *
 * @Author limingdong
 * @create 2022/9/21
 */
public class AbstractRowsFilterRuleForUdlThenUidTest extends AbstractEventTest {
    protected AbstractRowsFilterRule rowsFilterRule;
    // udl + uid
    protected static final String ROW_FILTER_PROPERTIES_V2 = "{\n" +
            "  \"rowsFilters\": [\n" +
            "    {\n" +
            "      \"mode\": \"%s\",\n" +
            "      \"tables\": \"drc1.insert1\",\n" +
            "      \"parameters\": null,\n" +
            "      \"configs\": {\n" +
            "        \"parameterList\": [\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"%s\"\n" + // udl
            "            ],\n" +
            "            \"illegalArgument\": false,\n" +
            "            \"context\": \"%s\",\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"userFilterMode\": \"udl\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"%s\"\n" + // uid
            "            ],\n" +
            "            \"illegalArgument\": false,\n" +
            "            \"context\": \"%s\",\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"userFilterMode\": \"uid\"\n" +
            "          }\n" +
            "        ],\n" +
            "        \"drcStrategyId\": 2000000002,\n" +
            "        \"routeStrategyId\": 0\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}\n" +
            "\n";


    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.TripUdlThenUid;
    }

    @Override
    protected String getProperties() {
        return ROW_FILTER_PROPERTIES_V2;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        rowsFilterRule = new UserRowsUdlThenUidFilterRule(dataMediaConfig.getRowsFilters().get(0));
    }

    public static class UdlExistTest extends AbstractRowsFilterRuleForUdlThenUidTest {
        @Override
        protected String getFullProperties() {
            return String.format(getProperties(), getRowsFilterType().getName(),
                    "foUR", getContext(), // udl exist and has value
                    "three_not_exist", getContext()); // uid
        }

        @Test
        public void filterRow() throws Exception {
            rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
            RowsFilterResult<List<AbstractRowsEvent.Row>> res = rowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
            Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
            Assert.assertEquals(1, res.getRes().size());
        }
    }

    public static class UdlNotExistThenUidTest extends AbstractRowsFilterRuleForUdlThenUidTest {
        @Override
        protected String getFullProperties() {
            return String.format(getProperties(), getRowsFilterType().getName(),
                    "four", getContext(), // udl // column exist but value all null of empty
                    "three", getContext()); // uid
        }

        @Override
        protected ByteBuf writeRowsEvent() {
            String hexString = "7d 1e 84 65   1e   ea 0c 00 00   65 00 00 00   28 cb fa 1f   00 00\n" +
                    "3d d9 03 00 00 00 01 00  02 00 06 ff 10 02 00 00 \n" +
                    "00 03 6f 6e 65 03 00 74  77 6f 01 31 65 83 fd 87 \n" +
                    "18 ba 10 03 00 00 00 03  6f 6e 65 03 00 74 77 6f \n" +
                    "01 33 65 83 fd 87 19 dc  10 04 00 00 00 03 6f 6e \n" +
                    "65 03 00 74 77 6f 01 35  65 83 fd 87 1a ea af 25 \n" +
                    "55 ff ";
            final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
            final byte[] bytes = toBytesFromHexString(hexString);
            byteBuf.writeBytes(bytes);

            return byteBuf;
        }

        @Test
        public void filterRow() throws Exception {
            rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
            RowsFilterResult<List<AbstractRowsEvent.Row>> res = rowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
            Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
            Assert.assertEquals(2, res.getRes().size());
        }
    }
}
