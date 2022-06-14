package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.ConnectionObservable;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.replicator.impl.inbound.handler.ColumnsQueryCommandExecutor;
import com.ctrip.framework.drc.replicator.impl.inbound.handler.QueryClientCommandHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.index.IndexExtractor;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.collect.Maps;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static ctrip.framework.drc.mysql.EmbeddedDb.*;

/**
 * @Author limingdong
 * @create 2020/5/27
 */
public class LocalSchemaManager extends MySQLSchemaManager {

    public LocalSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName, baseEndpointEntity);
    }

    @Override
    public boolean clone(Endpoint endpoint) {
        DDL_LOGGER.info("[clone] local schema manager, do nothing");
        return true;
    }

    @Override
    public Map<String, Map<String, String>> snapshot() {
        DDL_LOGGER.info("[snapshot] local schema manager, do nothing");
        return Maps.newHashMap();
    }

    @Override
    protected void doInitialize() {
        inMemoryEndpoint = new DefaultEndPoint(host, port, user, password);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
    }

    @Override
    protected void doDispose() {
        tableInfoMap.clear();
        inMemoryDataSource.close(true);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof ConnectionObservable) {
            SimpleObjectPool<NettyClient> simpleObjectPool = (SimpleObjectPool<NettyClient>) args;
            initTableInfo(simpleObjectPool);
            logger.info("[Update] TableInfo");
        }

    }

    private void initTableInfo(SimpleObjectPool<NettyClient> result) {
        FileManager fileManager = eventStore.getFileManager();
        File logDir = fileManager.getDataDir();
        File[] files = logDir.listFiles();
        if (files == null || files.length == 0) {
            columnSchema(result);
            for (Map.Entry<TableId, TableInfo> entry : tableInfoMap.entrySet()) {
                TableInfo tableInfo = entry.getValue();
                List<TableMapLogEvent.Column> columnList = tableInfo.getColumnList();
                try {
                    TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, tableInfo.getDbName(), tableInfo.getTableName(), columnList, null);
                    tableMapLogEvent.write(eventStore);
                    // release
                    tableMapLogEvent.release();
                } catch (IOException e) {
                    logger.error("TableMapLogEvent error", e);
                }
            }
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void columnSchema(SimpleObjectPool<NettyClient> simpleObjectPool) {

        QueryClientCommandHandler queryCommandHandler = new QueryClientCommandHandler();
        ColumnsQueryCommandExecutor queryCommandExecutor = new ColumnsQueryCommandExecutor(queryCommandHandler);
        CommandFuture<ResultSetPacket> commandFuture =  queryCommandExecutor.handle(simpleObjectPool);
        ResultSetPacket resultSetPacket;
        try {
            resultSetPacket = commandFuture.await().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long columnCount = resultSetPacket.getColumnCount();
        List<String> fieldValues = resultSetPacket.getFieldValues();

        for (int i = 0; i < fieldValues.size() / columnCount; ++i) {
            TableId tableId = new TableId();

            tableId.setDbName(fieldValues.get((int) (i * columnCount + 1)));
            tableId.setTableName(fieldValues.get((int) (i * columnCount + 2)));
            final String columnName = fieldValues.get((int) (i * columnCount + 3));
            final String columnDefault = fieldValues.get((int) (i * columnCount + 5));
            final boolean isNullable = "YES".equalsIgnoreCase(fieldValues.get((int) (i * columnCount + 6)));
            final String dataType = fieldValues.get((int) (i * columnCount + 7));
            final String characterOctetLength = fieldValues.get((int) (i * columnCount + 9));
            final String numberPrecision = fieldValues.get((int) (i * columnCount + 10));
            final String numberScale = fieldValues.get((int) (i * columnCount + 11));
            final String datetimePrecision = fieldValues.get((int) (i * columnCount + 12));
            final String charset = fieldValues.get((int) (i * columnCount + 13));
            final String collation = fieldValues.get((int) (i * columnCount + 14));
            final String columnType = fieldValues.get((int) (i * columnCount + 15));
            final String columnKey = fieldValues.get((int) (i * columnCount + 16));
            final String extra = fieldValues.get((int) (i * columnCount + 17));
            TableMapLogEvent.Column column = new TableMapLogEvent.Column(
                    columnName, isNullable, dataType, characterOctetLength,
                    numberPrecision, numberScale, datetimePrecision,
                    charset, collation, columnType, columnKey, extra, columnDefault
            );

            //7 是charset
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                tableInfo = new TableInfo();
                tableInfo.setDbName(tableId.getDbName());
                tableInfo.setTableName(tableId.getTableName());

                String indexQuery = String.format(INDEX_QUERY, tableId.getDbName(), tableId.getTableName());
                DataSource remoteDataSource = DataSourceManager.getInstance().getDataSource(endpoint);
                try (Connection connection = remoteDataSource.getConnection()) {
                    try (Statement statement = connection.createStatement()) {
                        try (ResultSet indexResultSet = statement.executeQuery(indexQuery)) {
                            List<List<String>> indexes = IndexExtractor.extractIndex(indexResultSet);
                            tableInfo.setIdentifiers(indexes);
                        }
                    }
                } catch (Exception e) {
                    logger.error("extractIndex error", e);
                }

                tableInfoMap.put(tableId, tableInfo);
            }
            tableInfo.addColumn(column);
        }

    }

}
