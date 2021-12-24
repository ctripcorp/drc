package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Dec 11, 2019
 */
public class ColumnsContainerTest implements ApplierColumnsRelatedTest {

    TestObject testObject;

    class TestObject implements ColumnsContainer {
        @Override
        public Columns getColumns() {
            return columns3();
        }
    }

    @Before
    public void setUp() throws Exception {
        testObject = new TestObject();
    }

    @Test
    public void getIdentifier() {
        assertEquals(Bitmap.fromMarks(0), testObject.getIdentifier(Lists.newArrayList(true, false, false, true)));
        assertEquals(Bitmap.fromMarks(2), testObject.getIdentifier(Lists.newArrayList(false, false, true, true, true)));
        assertEquals(Bitmap.fromMarks(3, 4), testObject.getIdentifier(Lists.newArrayList(false, false, false, true, true, false, true)));
    }

    @Test
    public void getIdentifierWhenSizeOfConditionBitmapIsSmallerThanSizeOfColumns() {
        assertNull(testObject.getIdentifier(Lists.newArrayList(false)));
        assertNull(testObject.getIdentifier(Lists.newArrayList(false, false, false, false, false)));
    }

    @Test
    public void getColumnOnUpdate() {
        assertEquals(Bitmap.fromMarks(6), testObject.getColumnOnUpdate(Lists.newArrayList(false, false, false, true, true, false, true)));
        assertEquals(null, testObject.getColumnOnUpdate(Lists.newArrayList(false, false, false, true, true, false, false, false)));
        assertEquals(Bitmap.fromMarks(7), testObject.getColumnOnUpdate(Lists.newArrayList(false, false, false, true, true, false, true, true)));
        assertEquals(Bitmap.fromMarks(6), testObject.getColumnOnUpdate(Lists.newArrayList(false, false, false, true, true, false, true, false)));
    }

}