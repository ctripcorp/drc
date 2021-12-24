package com.ctrip.framework.drc.core.driver.schema;

import com.ctrip.framework.drc.core.driver.schema.data.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Oct 13, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        BitmapTest.class,
        ColumnsTest.class,
        KeyTest.class,
        TableKeyTest.class,
        SchemasTest.class,
        FixedRAMSchemasHistoryTest.class,
})
public class SchemaTests {
}
