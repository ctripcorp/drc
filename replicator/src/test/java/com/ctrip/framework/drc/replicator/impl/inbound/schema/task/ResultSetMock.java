package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class ResultSetMock {

    private final Map<String, Integer> columnIndices;

    private final Object[][] data;

    private int rowIndex;

    private ResultSetMock(final String[] columnNames,
                          final Object[][] data) {
        // create a map of column name to column index
        this.columnIndices = IntStream.range(0, columnNames.length)
                .boxed()
                .collect(Collectors.toMap(
                        k -> columnNames[k],
                        Function.identity(),
                        (a, b) ->
                        {
                            throw new RuntimeException("Duplicate column " + a);
                        },
                        LinkedHashMap::new
                ));
        this.data = data;
        this.rowIndex = -1;
    }

    private ResultSet buildMock() throws SQLException {
        final var rs = mock(ResultSet.class);

        // mock rs.next()
        doAnswer(invocation -> {
            rowIndex++;
            return rowIndex < data.length;
        }).when(rs).next();
        // mock rs.getString(columnName)
        doAnswer(invocation -> {
            final var columnName = invocation.getArgument(0, String.class);
            final var columnIndex = columnIndices.get(columnName);
            return data[rowIndex][columnIndex];
        }).when(rs).getString(anyString());

        // mock rs.getString(int)
        doAnswer(invocation -> {
            final var columnIndex = invocation.getArgument(0, Integer.class);
            return data[rowIndex][columnIndex];
        }).when(rs).getString(anyInt());

        // mock rs.getObject(columnIndex)
        doAnswer(invocation -> {
            final var index = invocation.getArgument(0, Integer.class);
            return data[rowIndex][index - 1];
        }).when(rs).getObject(anyInt());

        final var rsmd = mock(ResultSetMetaData.class);

        // mock rsmd.getColumnCount()
        doReturn(columnIndices.size()).when(rsmd).getColumnCount();

        // mock rs.getMetaData()
        doReturn(rsmd).when(rs).getMetaData();

        return rs;
    }

    public static ResultSet create(String[] columnNames, Object[][] data) throws SQLException {
        return new ResultSetMock(columnNames, data).buildMock();
    }
}
