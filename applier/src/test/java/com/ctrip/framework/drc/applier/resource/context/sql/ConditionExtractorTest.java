package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Dec 10, 2019
 */
public class ConditionExtractorTest implements ApplierColumnsRelatedTest {

    @Test
    public void simpleUse() {
        List<List<Object>> rowsOfValues = Lists.newArrayList(
                Lists.newArrayList(1, "Phi", "2019-12-09 16:00:00.000"),
                Lists.newArrayList(2, "Phy", "2019-12-09 16:00:00.000"),
                Lists.newArrayList(3, "Torn", "2019-12-09 16:00:00.000")
        );
        List<Boolean> bitmapOfValues = Lists.newArrayList(true, true, false, true);
        ConditionExtractor extractor = new ConditionExtractor(columns1(), rowsOfValues, bitmapOfValues);
        assertEquals(Lists.newArrayList(
                true, false, false, true
        ), extractor.getBitmapOfConditions());
        assertEquals(Lists.newArrayList(
                Lists.newArrayList(1, "2019-12-09 16:00:00.000"),
                Lists.newArrayList(2, "2019-12-09 16:00:00.000"),
                Lists.newArrayList(3, "2019-12-09 16:00:00.000")
        ), extractor.getRowsOfConditions());

        assertEquals(Lists.newArrayList(
                true
        ), extractor.getBitmapOfIdentifier());
        assertEquals(Lists.newArrayList(
                Lists.newArrayList(1),
                Lists.newArrayList(2),
                Lists.newArrayList(3)
        ), extractor.getRowsOfIdentifier());

        assertEquals(Lists.newArrayList(
                false, false, false, true
        ), extractor.getBitmapOfColumnOnUpdate());
        assertEquals(Lists.newArrayList(
                "2019-12-09 16:00:00.000",
                "2019-12-09 16:00:00.000",
                "2019-12-09 16:00:00.000"
        ), extractor.getRowsOfColumnOnUpdate());

        assertNotSame(rowsOfValues, extractor.getRowsOfConditions());
        assertNotSame(bitmapOfValues, extractor.getBitmapOfConditions());

        //make sure that CE does no harm to origin data
        assertEquals(Lists.newArrayList(
                Lists.newArrayList(1, "Phi", "2019-12-09 16:00:00.000"),
                Lists.newArrayList(2, "Phy", "2019-12-09 16:00:00.000"),
                Lists.newArrayList(3, "Torn", "2019-12-09 16:00:00.000")
        ), rowsOfValues);
        assertEquals(Lists.newArrayList(true, true, false, true), bitmapOfValues);
    }
}