package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * try udl first, then uid second
 * @Author limingdong
 * @create 2022/9/21
 */
public class AbstractRowsFilterRuleForUidTest extends AbstractEventTest {

    private AbstractRowsFilterRule rowsFilterRule;

    // suppose one -> uid; two -> udl
    private static final String ROW_FILTER_PROPERTIES_V2 = "{\n" +
            "  \"rowsFilters\": [\n" +
            "    {\n" +
            "      \"mode\": \"%s\",\n" +
            "      \"tables\": \"drc1.insert1\",\n" +
            "      \"parameters\": {\n" +
            "        \"columns\": [\n" +
            "          \"three\"\n" +
            "        ],\n" +
            "        \"fetchMode\": 0,\n" +
            "        \"context\": \"%s\"\n" +
            "      },\n" +
            "      \"configs\": {\n" +
            "        \"parameterList\": [\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"three\"\n" +
            "            ],\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"context\": \"%s\",\n" +
            "            \"illegalArgument\": true,\n" +
            "            \"userFilterMode\": \"uid\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"four_no_exist\"\n" +
            "            ],\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"context\": \"%s\",\n" +
            "            \"illegalArgument\": true,\n" +
            "            \"userFilterMode\": \"udl\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"talbePairs\": [\n" +
            "    {\n" +
            "      \"source\": \"sourceTableName1\",\n" +
            "      \"target\": \"targetTableName1\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"source\": \"sourceTableName2\",\n" +
            "      \"target\": \"targetTableName2\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        rowsFilterRule = new UserRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.TripUdl;
    }

    @Override
    protected String getProperties() {
        return ROW_FILTER_PROPERTIES_V2;
    }

    @Override
    protected String getFullProperties() {
        return String.format(getProperties(), getRowsFilterType().getName(), getContext(), getContext(), getContext());
    }

    @Test
    public void filterRow() throws Exception {
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
        RowsFilterResult<List<AbstractRowsEvent.Row>> res = rowsFilterRule.filterRows(writeRowsEvent, rowsFilterContext);
        Assert.assertFalse(res.isNoRowFiltered().noRowFiltered());
        Assert.assertEquals(res.getRes().size(), 2);
    }

}
