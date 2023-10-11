package com.ctrip.framework.drc.console;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/10/10 14:14
 */
public class CommonTest {
    private static final String SELECT_SQL = "SELECT * FROM %s WHERE %s";

    @Test
    public void test01() {
        String updateSql = "/*DRC UPDATE 1*/update `migrationdb`.`benchmark` set id = 1 where id = 1 and name = 'name' and time <= '2023-09-28 16:06:15.019'";
        Map<String, String> parseResult = parseSql(updateSql);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(parseResult.get("operateType"));
        System.out.println(sql);

        String insertSql = "/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (1,1,'2023-09-28 16:06:15.019')";
        Map<String, String> parseResult1 = parseSql(insertSql);
        String sql1 = String.format(SELECT_SQL, parseResult1.get("tableName"), parseResult1.get("conditionStr"));
        System.out.println(parseResult1.get("operateType"));
        System.out.println(sql1);
    }

    @Test
    public void test02() {
        String insertSql = "/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (1,1,'2023-09-28 16:06:15.019')";
        Map<String, String> parseResult = parseSql(insertSql);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(parseResult.get("operateType"));
        System.out.println(sql);
    }

    @Test
    public void test03() {
        String updateSql = "/*DRC UPDATE 1*/update `migrationdb`.`benchmark` set id = 1 where name = 'name' and id = 1 and time <= '2023-09-28 16:06:15.019'";
        String updateFormatSql = SQLUtils.format(updateSql, JdbcConstants.MYSQL);
        System.out.println(updateFormatSql);
        System.out.println(updateFormatSql.startsWith("UPDATE"));
        String insertSql = "/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (1,1,'2023-09-28 16:06:15.019')";
        String insertFormatSql = SQLUtils.format(insertSql, JdbcConstants.MYSQL);
        System.out.println(insertFormatSql);
        System.out.println(insertFormatSql.startsWith("INSERT"));

    }

    private Map<String, String> parseSql(String sql) {
        Map<String, String> parseResult = new HashMap<>();

        String dbType = JdbcConstants.MYSQL;
        String formatSql = SQLUtils.format(sql, dbType);
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        if (formatSql.startsWith("UPDATE")) {
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);
            String tableName = visitor.getCurrentTable();
            parseResult.put("tableName", tableName);
            Map<TableStat.Name, TableStat> manipulationMap = visitor.getTables();
            String tableNameFormat = tableName.replace("`", "");
            TableStat.Name name = new TableStat.Name(tableNameFormat);
            TableStat stat = manipulationMap.get(name);
            parseResult.put("operateType", stat.toString());
            List<TableStat.Condition> Conditions = visitor.getConditions();
            TableStat.Condition equalCondition = Conditions.get(0);
            String equalConditionStr = equalCondition.toString();
            parseResult.put("conditionStr", equalConditionStr);
        } else if (formatSql.startsWith("INSERT")) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) stmt;
            insertStatement.getTableSource().toString();
            String tableName = insertStatement.getTableSource().toString();
            parseResult.put("tableName", tableName);

            List<SQLExpr> columns = insertStatement.getColumns();
            SQLExpr columnsExpr = columns.get(0);
            String column = columnsExpr.toString();

            List<SQLExpr> values = insertStatement.getValues().getValues();
            SQLExpr valueExpr = values.get(0);
            String value = valueExpr.toString();
            String conditionStr = column + " = " + value;
            parseResult.put("conditionStr", conditionStr);
            parseResult.put("operateType", "Insert");
        }

        return parseResult;
    }
}
