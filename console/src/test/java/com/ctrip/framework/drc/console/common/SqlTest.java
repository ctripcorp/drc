package com.ctrip.framework.drc.console.common;

import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/10/10 14:14
 */
public class SqlTest {
    private static final String SELECT_SQL = "SELECT * FROM %s WHERE %s";

    private static final String GET_ON_UPDATE_COLUMNS = "select column_name from information_schema.columns where table_schema='%s' and table_name='%s'";
    private static final String GET_COLUMN_PREFIX = "select column_name from information_schema.columns where table_schema='%s' and table_name='%s'";

    @Test
    public void testUpdate() {
        String updateSql = "/*DRC UPDATE 1*/update `migrationdb`.`benchmark` set id = 1 where `id` = 1 and `name` = 'name' and `datachange_lasttime` <= '2023-09-28 16:06:15.019' AND uid in (1, 2, 3)";
        Map<String, String> parseResult = MySqlUtils.parseSql(updateSql, Lists.newArrayList("`datachange_lasttime`"), Lists.newArrayList("`id`", "`name`"));
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);
        System.out.println(parseResult);

        Assert.assertEquals("SELECT * FROM migrationdb.benchmark WHERE id='1' AND `name`='name'", sql);
        Assert.assertEquals("id='1' AND `name`='name'", parseResult.get("conditionStr"));
        Assert.assertEquals("Update", parseResult.get("operateType"));
        Assert.assertEquals("migrationdb.benchmark", parseResult.get("tableName"));
    }

    @Test
    public void testInsert() {
        String insertSql = "/*DRC UPDATE 2*/ insert into `migrationdb`.`benchmark` (`id`, `name`, `desc`, `datachange_lasttime`) values (1, 'name', 'desc', '2023-09-28 16:06:15.019');";
        Map<String, String> parseResult = MySqlUtils.parseSql(insertSql, Lists.newArrayList("`datachange_lasttime`"), Lists.newArrayList("`id`", "`name`"));
        System.out.println(parseResult);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);

        Assert.assertEquals("SELECT * FROM `migrationdb`.`benchmark` WHERE `id` = 1 AND `name` = 'name'", sql);
    }

    @Test
    public void testDelete() {
        String deleteSql = "/*DRC DELETE 1*/delete from `migrationdb`.`benchmark` where `name` = 'name' and `id` = 1 and `datachange_lasttime` <= '2023-09-28 16:06:15.019'";
        Map<String, String> parseResult = MySqlUtils.parseSql(deleteSql, Lists.newArrayList("`datachange_lasttime`"), Lists.newArrayList("`id`", "`name`"));
        System.out.println(parseResult);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);

        Assert.assertEquals("SELECT * FROM migrationdb.benchmark WHERE `name`='name' AND `id`='1'", sql);
        Assert.assertEquals("`name`='name' AND `id`='1'", parseResult.get("conditionStr"));
        Assert.assertEquals("Delete", parseResult.get("operateType"));
        Assert.assertEquals("migrationdb.benchmark", parseResult.get("tableName"));
    }

}
