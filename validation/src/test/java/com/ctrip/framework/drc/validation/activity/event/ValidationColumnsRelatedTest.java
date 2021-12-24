package com.ctrip.framework.drc.validation.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/28
 */
public interface ValidationColumnsRelatedTest {

    default Columns columns0() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"user\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"lt\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }

    default Columns columns1() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"user\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"gender\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"lt\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }

    default Columns columns2() {
        String json2 = "{\"columns\":[" +
                "{\"type\":15,\"meta\":150,\"nullable\":false,\"name\":\"hostname\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"time\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json2.getBytes(), ColumnContainer.class).columns);
    }

    default Columns columns3() {
        Columns columns = mock(Columns.class);
        when(columns.getBitmapsOfIdentifier()).thenReturn(Lists.newArrayList(
                Bitmap.fromMarks(0),
                Bitmap.fromMarks(2),
                Bitmap.fromMarks(3,4)
        ));
        when(columns.getBitmapsOnUpdate()).thenReturn(Lists.newArrayList(
                Bitmap.fromMarks(6),
                Bitmap.fromMarks(7)
        ));
        return columns;
    }

    default Columns columns4() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"src_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"dest_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"datachange_lasttime\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }

    default Columns columns5() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"src_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"dest_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"none\",\"columnDefault\":\"none\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"none2\",\"columnDefault\":\"none2\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"datachange_lasttime\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }

    default Columns columns6() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"user\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"gender\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"uid1\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"lt\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }

    class ColumnContainer {
        public List<TableMapLogEvent.Column> getColumns() {
            return columns;
        }

        public void setColumns(List<TableMapLogEvent.Column> columns) {
            this.columns = columns;
        }

        public List<TableMapLogEvent.Column> columns;
    }

}
