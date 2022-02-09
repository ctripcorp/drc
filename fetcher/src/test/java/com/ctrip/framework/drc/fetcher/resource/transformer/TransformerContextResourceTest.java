package com.ctrip.framework.drc.fetcher.resource.transformer;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by jixinwang on 2022/1/25
 */
public class TransformerContextResourceTest {

    @Test
    public void testParseNameMapping() {
        TransformerContextResource transformerContextResource = new TransformerContextResource();
        transformerContextResource.setNameMapping("test1[01-16]db.testTable1[1-8],test2[01-16]db.testTable2[1-8];");
        transformerContextResource.parseNameMapping();
        Map<String, String> nameMap = transformerContextResource.getNameMap();
        assertEquals(128, nameMap.size());
    }
}
