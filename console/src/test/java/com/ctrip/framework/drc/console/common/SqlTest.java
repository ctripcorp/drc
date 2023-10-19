package com.ctrip.framework.drc.console.common;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<String, String> parseResult = parseSql(updateSql);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);
        System.out.println(parseResult);

    }

    @Test
    public void testInsert() {
        String insertSql = "/*DRC UPDATE 2*/ INSERT INTO `bbzimelongdetailshard05db`.`adviceofread` (`id`,`cid`,`ownerId`,`partnerId`,`type`,`messageCreateTime`,`DataChange_LastTime`,`dbshard`,`id_bak`,`ver_a`,`ver_b`) VALUES (115374965,'openim1528979669764ial96mqt0mq00:1399054765677084752:1','openim1528979669764ial96mqt0mq00','1399054765677084752',1,1697710421615,'2023-10-19 20:56:33',null,null,'2020-02-02 00:00:00','2023-10-19 20:56:33')";
        String insertFormatSql = SQLUtils.format(insertSql, JdbcConstants.MYSQL);
        Map<String, String> parseResult = parseSql(insertSql);
        System.out.println(parseResult);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);

    }

    @Test
    public void testDelete() {
        String deleteSql = "/*DRC DELETE 1*/delete from `migrationdb`.`benchmark` where `name` = 'name' and `id` = 1 and `datachange_lasttime` <= '2023-09-28 16:06:15.019'";
        String sqlFormat = SQLUtils.format(deleteSql, JdbcConstants.MYSQL);
        System.out.println(sqlFormat);
        System.out.println(sqlFormat.startsWith("DELETE"));
        Map<String, String> parseResult = parseSql(deleteSql);
        System.out.println(parseResult);
        String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("conditionStr"));
        System.out.println(sql);
    }


    private Map<String, String> parseSql(String sql) {
        Map<String, String> parseResult = new HashMap<>();

        String dbType = JdbcConstants.MYSQL;
        String formatSql = SQLUtils.format(sql, dbType);
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        if (formatSql.startsWith("UPDATE") || formatSql.startsWith("DELETE")) {
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);
            String tableName = visitor.getCurrentTable();
            parseResult.put("tableName", tableName);
            Map<TableStat.Name, TableStat> manipulationMap = visitor.getTables();
            String tableNameFormat = tableName.replace("`", "");
            TableStat.Name name = new TableStat.Name(tableNameFormat);
            TableStat stat = manipulationMap.get(name);
            parseResult.put("operateType", stat.toString());
            List<TableStat.Condition> conditions = visitor.getConditions();

            conditions = conditions.stream().filter(e -> !e.getColumn().getName().equals("`datachange_lasttime`")).collect(Collectors.toList());
            boolean firstCondition = true;
            StringBuilder whereCondition = new StringBuilder();
            for (TableStat.Condition condition : conditions) {
                if (!"=".equals(condition.getOperator())) {
                    continue;
                }
                if (!firstCondition) {
                    whereCondition.append(" AND ");
                }
                String column = condition.getColumn().getName();
                String value = condition.getValues().get(0).toString();
                whereCondition.append(column + "=" + toSqlValue(value));
                firstCondition = false;
            }
//            String equalConditionStr = Joiner.on(" AND ").join(conditions);
//            parseResult.put("conditionStr", equalConditionStr);
            parseResult.put("conditionStr", whereCondition.toString());
        } else if (formatSql.startsWith("INSERT")) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) stmt;
            insertStatement.getTableSource().toString();
            String tableName = insertStatement.getTableSource().toString();
            parseResult.put("tableName", tableName);


            List<SQLExpr> columns = insertStatement.getColumns();
            List<SQLExpr> values = insertStatement.getValues().getValues();
            boolean firstCondition = true;
            StringBuilder condition = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i).toString();
                if (columnName.equals("`datachange_lasttime`")) {
                    continue;
                }
                String value = values.get(i).toString();
                SQLExpr sqlExpr = values.get(i);
                if (sqlExpr instanceof SQLNullExpr) {
                    System.out.println(sqlExpr);
                }
                if (!firstCondition) {
                    condition.append(" AND ");
                }
                condition.append(columnName + " = " + value);
                firstCondition = false;
            }

            parseResult.put("conditionStr", condition.toString());
            parseResult.put("operateType", "Insert");
        }

        return parseResult;
    }

    @Test
    public void test() {
        String db = "db";
        String table = "table";
        String sql = String.format(GET_ON_UPDATE_COLUMNS, db, table);
        System.out.println(sql);
    }

    private String toSqlValue(String val) {
        return "'" + val + "'";
    }
}
