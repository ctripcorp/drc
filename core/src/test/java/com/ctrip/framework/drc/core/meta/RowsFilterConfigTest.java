package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.xpipe.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class RowsFilterConfigTest {

    /**
     * {
     *   "mode": "trip_udl",
     *   "tables": "drc1.insert1",
     *   "parameters": {
     *     "columns": [
     *       "id",
     *       "one"
     *     ],
     *     "context": "regre2"
     *   },
     *   "configs": {
     *     "parameterList": [
     *       {
     *         "columns": [
     *           "uid"
     *         ],
     *         "context": "SIN",
     *         "illegalArgument": true,
     *         "userFilterMode": "uid"
     *       },
     *       {
     *         "columns": [
     *           "udl"
     *         ],
     *         "userFilterMode": "udl",
     *         "illegalArgument": false,
     *         "context": "SIN"
     *       }
     *     ]
     *   }
     * }
     */

    public static final String OLD_MEDIA_CONFIG = "{" +
            "      \"mode\": \"java_regex\"," +
            "      \"tables\": \"drc1.insert1\"," +
            "      \"parameters\": {" +
            "        \"columns\": [" +
            "          \"id\"," +
            "          \"one\"" +
            "        ]," +
            "        \"context\": \"regre2\"" +
            "      }" +
            "    }";

    public static final String NEW_MEDIA_CONFIG = "{\n" +
            "  \"mode\": \"trip_udl\",\n" +
            "  \"tables\": \"drc1.insert1\",\n" +
            "  \"parameters\": {\n" +
            "    \"columns\": [\n" +
            "      \"id\",\n" +
            "      \"one\"\n" +
            "    ],\n" +
            "    \"context\": \"regre2\"\n" +
            "  },\n" +
            "  \"configs\": {\n" +
            "    \"parameterList\": [\n" +
            "      {\n" +
            "        \"columns\": [\n" +
            "          \"uid\"\n" +
            "        ],\n" +
            "        \"context\": \"SIN\",\n" +
            "        \"illegalArgument\": true,\n" +
            "        \"userFilterMode\": \"uid\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"columns\": [\n" +
            "          \"udl\"\n" +
            "        ],\n" +
            "        \"userFilterMode\": \"udl\",\n" +
            "        \"illegalArgument\": false,\n" +
            "        \"context\": \"SIN\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    private RowsFilterConfig rowsFilterConfigOld;

    private RowsFilterConfig rowsFilterConfigNew;

    @Before
    public void setUp() throws Exception {
        rowsFilterConfigOld = JsonCodec.INSTANCE.decode(OLD_MEDIA_CONFIG, RowsFilterConfig.class);
        rowsFilterConfigNew = JsonCodec.INSTANCE.decode(NEW_MEDIA_CONFIG, RowsFilterConfig.class);
    }

    @Test
    public void getRowsFilterTypeForOld() {
        Assert.assertNull(rowsFilterConfigOld.getParameters());
        RowsFilterType type = rowsFilterConfigOld.getRowsFilterType();
        Assert.assertEquals(RowsFilterType.JavaRegex, type);
        String tables = rowsFilterConfigOld.getTables();
        Assert.assertEquals("drc1.insert1", tables);
    }

    @Test
    public void getRowsFilterTypeForNew() {
        List<String> columns = rowsFilterConfigNew.getParameters().getColumns();
        Assert.assertEquals(1, columns.size());
        RowsFilterType type = rowsFilterConfigNew.getRowsFilterType();
        Assert.assertEquals(RowsFilterType.TripUdl, type);
        String expression = rowsFilterConfigNew.getParameters().getContext();
        Assert.assertEquals("SIN", expression);
        String tables = rowsFilterConfigNew.getTables();
        Assert.assertEquals("drc1.insert1", tables);

        List<RowsFilterConfig.Parameters> parametersList = rowsFilterConfigNew.getConfigs().getParameterList();
        Assert.assertEquals(2, parametersList.size());
        Assert.assertEquals("uid", parametersList.get(0).getColumns().get(0));
        Assert.assertEquals("udl", parametersList.get(1).getColumns().get(0));

        Collections.sort(parametersList);
        Assert.assertEquals("udl", parametersList.get(0).getColumns().get(0));
        Assert.assertEquals("uid", parametersList.get(1).getColumns().get(0));
    }

}