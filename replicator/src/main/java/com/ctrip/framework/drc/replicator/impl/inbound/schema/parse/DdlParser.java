package com.ctrip.framework.drc.replicator.impl.inbound.schema.parse;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item;
import com.alibaba.fastsql.sql.parser.ParserException;
import com.alibaba.fastsql.util.JdbcConstants;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter.DEFAULT_CHARACTER_SET_SERVER;

/**
 * @Author limingdong
 * @create 2020/2/24
 */
public class DdlParser {

    private static final String CHARSET = "CHARSET";

    public static List<DdlResult> parse(String queryString, String schmeaName) {

        List<SQLStatement> stmtList;

        try {
            stmtList = SQLUtils.parseStatements(queryString, JdbcConstants.MYSQL, false);
        } catch (ParserException e) {
            // maybe sp that not support
            DdlResult ddlResult = new DdlResult();
            ddlResult.setType(QueryType.QUERY);
            return Arrays.asList(ddlResult);
        }

        List<DdlResult> ddlResults = Lists.newArrayList();

        for (SQLStatement statement : stmtList) {
            if (statement instanceof SQLCreateTableStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLCreateTableStatement createTable = (SQLCreateTableStatement) statement;
                processName(ddlResult, schmeaName, createTable.getName(), false);
                if (((SQLCreateTableStatement) statement).getLike() == null) {  //create xx like xxx, set DEFAULT_CHARACTER_SET_SERVER
                    processTableCharset(ddlResult, createTable.getTableOptions());
                } else {
                    ddlResult.setTableCharset(DEFAULT_CHARACTER_SET_SERVER);
                }
                ddlResult.setType(QueryType.CREATE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
                for (SQLAlterTableItem item : alterTable.getItems()) {
                    if (item instanceof SQLAlterTableRename) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), true);
                        processName(ddlResult, schmeaName, ((SQLAlterTableRename) item).getToName(), false);
                        ddlResult.setType(QueryType.RENAME);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableAddIndex) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(QueryType.CINDEX);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableDropIndex || item instanceof SQLAlterTableDropKey) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(QueryType.DINDEX);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableAddConstraint) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        SQLConstraint constraint = ((SQLAlterTableAddConstraint) item).getConstraint();
                        if (constraint instanceof SQLUnique) {
                            ddlResult.setType(QueryType.CINDEX);
                            ddlResults.add(ddlResult);
                        }
                    } else if (item instanceof SQLAlterTableDropConstraint) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(QueryType.DINDEX);
                        ddlResults.add(ddlResult);
                    } else {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(QueryType.ALTER);
                        ddlResults.add(ddlResult);
                    }
                }
            } else if (statement instanceof SQLDropTableStatement) {
                SQLDropTableStatement dropTable = (SQLDropTableStatement) statement;
                for (SQLExprTableSource tableSource : dropTable.getTableSources()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                    ddlResult.setType(QueryType.ERASE);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof SQLCreateIndexStatement) {
                SQLCreateIndexStatement createIndex = (SQLCreateIndexStatement) statement;
                SQLTableSource tableSource = createIndex.getTable();
                DdlResult ddlResult = new DdlResult();
                processName(ddlResult, schmeaName, ((SQLExprTableSource) tableSource).getExpr(), false);
                ddlResult.setType(QueryType.CINDEX);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDropIndexStatement) {
                SQLDropIndexStatement dropIndex = (SQLDropIndexStatement) statement;
                SQLExprTableSource tableSource = dropIndex.getTableName();
                DdlResult ddlResult = new DdlResult();
                processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                ddlResult.setType(QueryType.DINDEX);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLTruncateStatement) {
                SQLTruncateStatement truncate = (SQLTruncateStatement) statement;
                for (SQLExprTableSource tableSource : truncate.getTableSources()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                    ddlResult.setType(QueryType.TRUNCATE);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof MySqlRenameTableStatement) {
                MySqlRenameTableStatement rename = (MySqlRenameTableStatement) statement;
                for (Item item : rename.getItems()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, item.getName(), true);
                    processName(ddlResult, schmeaName, item.getTo(), false);
                    ddlResult.setType(QueryType.RENAME);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof SQLInsertStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLInsertStatement insert = (SQLInsertStatement) statement;
                processName(ddlResult, schmeaName, insert.getTableName(), false);
                ddlResult.setType(QueryType.INSERT);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLUpdateStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLUpdateStatement update = (SQLUpdateStatement) statement;
                // 拿到的表名可能为null,比如update a,b set a.id=x
                processName(ddlResult, schmeaName, update.getTableName(), false);
                ddlResult.setType(QueryType.UPDATE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDeleteStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLDeleteStatement delete = (SQLDeleteStatement) statement;
                // 拿到的表名可能为null,比如delete a,b from a where a.id = b.id
                processName(ddlResult, schmeaName, delete.getTableName(), false);
                ddlResult.setType(QueryType.DELETE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLCreateDatabaseStatement) {
                SQLCreateDatabaseStatement create = (SQLCreateDatabaseStatement) statement;
                DdlResult ddlResult = new DdlResult();
                ddlResult.setType(QueryType.QUERY);
                processName(ddlResult, create.getDatabaseName(), null, false);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDropDatabaseStatement) {
                SQLDropDatabaseStatement drop = (SQLDropDatabaseStatement) statement;
                DdlResult ddlResult = new DdlResult();
                ddlResult.setType(QueryType.QUERY);
                processName(ddlResult, drop.getDatabaseName(), null, false);
                ddlResults.add(ddlResult);
            }
        }

        return ddlResults;
    }

    private static void processName(DdlResult ddlResult, String schema, SQLExpr sqlName, boolean isOri) {
        if (sqlName == null) {
            if (StringUtils.isNotBlank(schema)) {
                ddlResult.setSchemaName(schema);
            }
            return;
        }

        String table = null;
        if (sqlName instanceof SQLPropertyExpr) {
            SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
            schema = unescapeName(owner.getName());
            table = unescapeName(((SQLPropertyExpr) sqlName).getName());
        } else if (sqlName instanceof SQLIdentifierExpr) {
            table = unescapeName(((SQLIdentifierExpr) sqlName).getName());
        }

        if (isOri) {
            ddlResult.setOriSchemaName(schema);
            ddlResult.setOriTableName(table);
        } else {
            ddlResult.setSchemaName(schema);
            ddlResult.setTableName(table);
        }
    }

    public static void processTableCharset(DdlResult ddlResult, List<SQLAssignItem> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }
        for (SQLAssignItem item : tableOptions) {
            SQLExpr target = item.getTarget();
            if (target instanceof SQLIdentifierExpr) {
                String key = unescapeName(((SQLIdentifierExpr) target).getName());
                if(CHARSET.equalsIgnoreCase(key)) {
                    String charset = unescapeName(((SQLIdentifierExpr) item.getValue()).getName());
                    ddlResult.setTableCharset(charset);
                    return;
                }
            }
        }
    }

    public static String appendTableCharset(String query, String charset) {
        if (StringUtils.isBlank(charset)) {
            return query;
        }
        query = query.trim();
        if (query.endsWith(";")) {
            int index = query.lastIndexOf(";");
            query = query.substring(0, index);
        }

        return query + getAppendCharset(charset);
    }

    public static String getAppendCharset(String charset) {
        return String.format(" %s=%s;", CHARSET, charset);
    }

    public static String unescapeName(String name) {
        if (name != null && name.length() > 2) {
            char c0 = name.charAt(0);
            char x0 = name.charAt(name.length() - 1);
            if ((c0 == '"' && x0 == '"') || (c0 == '`' && x0 == '`')) {
                return name.substring(1, name.length() - 1);
            }
        }

        return name;
    }

    public static String unescapeQuotaName(String name) {
        if (name != null && name.length() > 2) {
            char c0 = name.charAt(0);
            char x0 = name.charAt(name.length() - 1);
            if (c0 == '\'' && x0 == '\'') {
                return name.substring(1, name.length() - 1);
            }
        }

        return name;
    }

}
