package com.ctrip.framework.drc.core.driver.schema.data;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.schema.data.Bitmap.from;
import static org.junit.Assert.*;

/**
 * @Author Slight
 * Jul 02, 2020
 */
public class BitmapTest {

    @Test
    public void union() {
        List<Boolean> a = Lists.newArrayList(true, false, false);
        List<Boolean> b = Lists.newArrayList(true, false, true);
        assertEquals("[true, false, true]", Bitmap.union(a, b).toString());

        a = Lists.newArrayList(true, false, false);
        b = Lists.newArrayList(true, false, true, true);
        assertEquals("[true, false, true, true]", Bitmap.union(a, b).toString());

        a = Lists.newArrayList(true, false, true, true);
        b = Lists.newArrayList(true, false, false);
        assertEquals("[true, false, true, true]", Bitmap.union(a, b).toString());

        a = Lists.newArrayList(false, false, false, true);
        b = Lists.newArrayList(false, false, false);
        assertEquals("[false, false, false, true]", Bitmap.union(a, b).toString());
    }

    @Test
    public void isSubsetOf() {
        List<Boolean> a = Lists.newArrayList(true, false, false);
        List<Boolean> b = Lists.newArrayList(true, false, true);
        assertTrue(from(a).isSubsetOf(from(b)));

        a = Lists.newArrayList(true, false, false, false);
        b = Lists.newArrayList(true, false, true);
        assertTrue(from(a).isSubsetOf(from(b)));

        a = Lists.newArrayList(false, false, false, false, true);
        b = Lists.newArrayList(true, false, true, false);
        assertFalse(from(a).isSubsetOf(from(b)));

        a = Lists.newArrayList(false, true, false);
        b = Lists.newArrayList(true, false, false, false);
        assertFalse(from(a).isSubsetOf(from(b)));
    }

    @Test
    public void on() {
        List<String> data = Lists.newArrayList("A", "B", "C");
        Bitmap a = Bitmap.from(true, false, false);
        Bitmap b = Bitmap.from(true, false);
        Bitmap c = Bitmap.from(true, false, false, false);
        Bitmap d = Bitmap.from(true, false, false, true);
        Bitmap e = Bitmap.from(false, false, false, false);
        Bitmap f = Bitmap.from(true, true);
        Bitmap g = Bitmap.from(true, true, true, true);
        assertEquals("[A]", a.on(data).toString());
        assertEquals("[A]", b.on(data).toString());
        assertEquals("[A]", c.on(data).toString());
        assertEquals("[A]", d.on(data).toString());
        assertEquals("[]", e.on(data).toString());
        assertEquals("[A, B]", f.on(data).toString());
        assertEquals("[A, B, C]", g.on(data).toString());
    }

    @Test
    public void fromMarks() {
        assertEquals("[false, true, true]", Bitmap.fromMarks(2, 1).toString());
        assertEquals("[false, false, false, true]", Bitmap.fromMarks(3).toString());
        assertEquals("[true, false, false, true]", Bitmap.fromMarks(0, 3).toString());
        assertEquals("[true, true]", Bitmap.fromMarks(0, 1).toString());
    }

    @Test
    public void subAndOrigin() {
        assertEquals("[true, false, true]", Bitmap.from(
                Lists.newArrayList("a", "c"),
                Lists.newArrayList("a", "b", "c")
        ).toString());
    }

    @Test
    public void under() {
        assertEquals(
                "[0, 1, 4]",
                Bitmap.fromMarks(2, 3).under(
                        Lists.newArrayList(0, 1, 2, 3, 4)
                ).toString());

        assertEquals(
                "[0, 1, 4]",
                Bitmap.fromMarks(2, 3, 5).under(
                        Lists.newArrayList(0, 1, 2, 3, 4)
                ).toString());

        assertEquals(
                "[0, 1, 2, 3, 4]",
                Bitmap.fromMarks(5, 6, 7).under(
                        Lists.newArrayList(0, 1, 2, 3, 4)
                ).toString());
    }

    @Test
    public void testClone() {
        Bitmap a = Bitmap.from(true, false, false);
        Bitmap aClone = (Bitmap) a.clone();
        Assert.assertArrayEquals(a.toArray(), aClone.toArray());
        Assert.assertTrue(a != aClone);

        aClone.remove(2);
        Assert.assertTrue(a.size() == 3);
        Assert.assertTrue(aClone.size() == 2);
    }
}