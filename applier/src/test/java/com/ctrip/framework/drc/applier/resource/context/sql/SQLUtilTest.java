package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.sql.SQLUtil;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 09, 2019
 */
public class SQLUtilTest {

    SQLUtil builder;

    @Test
    public void bracket() {
        assertEquals("()", builder.bracket(""));
        assertEquals("(hello bracket)", builder.bracket("hello bracket"));
        assertEquals("(username,password)", builder.bracket("username,password"));
    }

    @Test
    public void groupColumnNames() {
        assertEquals("()", builder.groupColumnNames(buildArray()));
        assertEquals("(`user`,`password`)", builder.groupColumnNames(buildArray(
                "user", "password")));
    }

    @Test
    public void groupColumnNamesWithBitmap() {
        assertEquals("(`user`,`gender`)", builder.groupColumnNames(buildArray(
                "user", "password", "gender"), buildArray(true, false, true)));
    }

    @Test
    public void prepareRowsOfValues() {
        assertEquals("", builder.prepareRowsOfValues(buildArray()));
        assertEquals("()", builder.prepareRowsOfValues(buildArray(buildArray())));
        assertEquals("(),()", builder.prepareRowsOfValues(buildArray(buildArray(), buildArray())));
        assertEquals("(?),(?)", builder.prepareRowsOfValues(buildArray(buildArray(0), buildArray(1))));
        assertEquals("(?,?),(?,?)", builder.prepareRowsOfValues(buildArray(buildArray(0, "Mag"), buildArray(1, "Phy"))));
        assertEquals("(?,?),(?,?,?)", builder.prepareRowsOfValues(buildArray(buildArray(0, "Mag"), buildArray(1, "Phy", "Male"))));
    }

    @Test
    public void prepareEquations() {
        assertEquals("`id`=? AND `last_updated`=?",
                builder.prepareEquations(Lists.newArrayList("id", "last_updated"), " AND "));
        assertEquals("`id`=?,`last_updated`=?",
                builder.prepareEquations(Lists.newArrayList("id", "last_updated"), ","));
    }

    @Test
    public void groupQuestionMarks() {
        assertEquals("()", builder.groupQuestionMarks(0));
        assertEquals("(?)", builder.groupQuestionMarks(1));
        assertEquals("(?,?)", builder.groupQuestionMarks(2));
        assertEquals("(?,?,?)", builder.groupQuestionMarks(3));
    }

    @Test
    public void selectColumnNames() {
        List<String> selectedColumnNames = builder.selectColumnNames(
                buildArray("user", "password", "gender"),
                buildArray(true, false, true)
        );
        assertEquals("user", selectedColumnNames.get(0));
        assertEquals("gender", selectedColumnNames.get(1));
    }

    @Test
    public void selectColumnNamesWithNullBitmap() {
        List<String> selectedColumnNames = builder.selectColumnNames(
                buildArray("user", "password", "gender"),
                null
        );
        assertEquals("user", selectedColumnNames.get(0));
        assertEquals("password", selectedColumnNames.get(1));
        assertEquals("gender", selectedColumnNames.get(2));
    }

    @Before
    public void setUp() throws Exception {
        builder = new SQLUtil() {
        };
    }

    @After
    public void tearDown() throws Exception {
        builder = null;
    }

    protected <T extends Object> ArrayList<T> buildArray(T... items) {
        ArrayList<T> list = new ArrayList();
        if (items == null) {
            list.add(null);
            return list;
        }
        for (T item : items) {
            list.add(item);
        }
        return list;
    }
}