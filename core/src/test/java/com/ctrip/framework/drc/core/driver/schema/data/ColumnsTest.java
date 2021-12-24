package com.ctrip.framework.drc.core.driver.schema.data;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent.Column;
import com.ctrip.xpipe.api.codec.Codec;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Aug 12, 2020
 */
public class ColumnsTest {

    public static class ColumnContainer {
        public List<Column> getColumns() {
            return columns;
        }

        public void setColumns(List<Column> columns) {
            this.columns = columns;
        }

        public List<Column> columns;
    }

    public List<Column> columns1() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"user\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"gender\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"lt\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Codec.DEFAULT.decode(json.getBytes(), ColumnContainer.class).columns;
    }

    @Test
    public void testBitmap() {
        Columns columns = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender")
        ));
        assertEquals("[[false, false, false, true]]", columns.getBitmapsOnUpdate().toString());
        assertEquals(2, columns.getBitmapsOfIdentifier().size());
        assertEquals("[true]", columns.getBitmapsOfIdentifier().get(0).toString());
        assertEquals("[false, true, true, false]", columns.getBitmapsOfIdentifier().get(1).toString());
    }

    @Test
    public void testEquals() {
        Columns a = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender")
        ));
        Columns b = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender")
        ));
        Columns c = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender"),
                Lists.newArrayList("id", "gender")
        ));
        assertEquals(a, b);
        assertNotEquals(a, c);
    }

    @Test
    public void testClone() {
        Columns a = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender")
        ));
        Columns aclone = (Columns) a.clone();
        assertEquals(a, aclone);

        aclone.removeColumn("user");
        assertNotEquals(a, aclone);

        Columns c = Columns.from(columns1(), Lists.newArrayList(
                Lists.newArrayList("id"),
                Lists.newArrayList("user", "gender"),
                Lists.newArrayList("id", "gender")
        ));
        c.getBitmapsOfIdentifier();
        c.getBitmapsOnUpdate();
        Columns cclone = (Columns) c.clone();
        assertEquals(c, cclone);

        c.removeColumn("gender");
        assertNotEquals(c, cclone);
    }


    @Test
    public void testUnion() {
        List<String> list = Lists.newArrayList("id");
        List<List<String>> values = Lists.newArrayList();
        values.add(list);
        Columns columns = Columns.from(columns1(), values);

        Lists.newArrayList(Lists.newArrayList("id"));
        Bitmap bitmapOfConditions = Bitmap.union(
                columns.getBitmapsOfIdentifier().get(0),
                columns.getLastBitmapOnUpdate()
        );
        Assert.assertTrue(bitmapOfConditions.size() == 4);
        Assert.assertTrue(bitmapOfConditions.get(0));  //id
        Assert.assertTrue(bitmapOfConditions.get(3));  // on update
    }
}