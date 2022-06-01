package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.xpipe.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class RowsFilterConfigTest {

    public static final String MEDIA_CONFIG = "{" +
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

    private RowsFilterConfig rowsFilterConfig;

    @Before
    public void setUp() throws Exception {
        rowsFilterConfig = JsonCodec.INSTANCE.decode(MEDIA_CONFIG, RowsFilterConfig.class);
    }

    @Test
    public void getRowsFilterType() {
        List<String> columns = rowsFilterConfig.getParameters().getColumns();
        Assert.assertEquals(2, columns.size());
        RowsFilterType type = rowsFilterConfig.getRowsFilterType();
        Assert.assertEquals(RowsFilterType.JavaRegex, type);
        String expression = rowsFilterConfig.getParameters().getContext();
        Assert.assertEquals("regre2", expression);
        String tables = rowsFilterConfig.getTables();
        Assert.assertEquals("drc1.insert1", tables);
    }

}