package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.xpipe.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class RowsFilterConfigsTest {

    public static final String MEDIA_CONFIG = "{\n" +
            "  \"rowsFilters\": [\n" +
            "    {\n" +
            "      \"mode\": \"trip_uid\",\n" +
            "      \"tables\": \"table1\",\n" +
            "      \"parameters\": {\n" +
            "        \"columns\": [\n" +
            "          \"columnA\",\n" +
            "          \"columnB\",\n" +
            "          \"cloumnC\"\n" +
            "        ],\n" +
            "        \"expression\": \"regre1\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"mode\": \"aviator_regex\",\n" +
            "      \"tables\": \"table2\",\n" +
            "      \"parameters\": {\n" +
            "        \"columns\": [\n" +
            "          \"cloumnA\"\n" +
            "        ],\n" +
            "        \"expression\": \"regre2\"\n" +
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

    private DataMediaConfig rowsFilterConfigs;

    @Before
    public void setUp() throws Exception {
        rowsFilterConfigs = JsonCodec.INSTANCE.decode(MEDIA_CONFIG, DataMediaConfig.class);
    }

    @Test
    public void getRowsFilterType() {
        List<RowsFilterConfig> rowsFilters = rowsFilterConfigs.getRowsFilters();
        Assert.assertEquals(2, rowsFilters.size());
        Assert.assertTrue(rowsFilterConfigs.shouldFilterRows());
    }
}