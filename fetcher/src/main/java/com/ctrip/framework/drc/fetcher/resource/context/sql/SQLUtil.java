package com.ctrip.framework.drc.fetcher.resource.context.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author Slight
 * Oct 09, 2019
 */
public interface SQLUtil {

    //Make sure the size of columnNames and the size of bitmap are the same.
    default String groupColumnNames(List<String> columnNames, List<Boolean> bitmap) {
        return groupColumnNames(selectColumnNames(columnNames, bitmap));
    }

    default String groupColumnNames(List<String> columnNames) {
        return bracket(join(columnNames, "`", ","));
    }

    default String prepareEquations(List<String> keys, String deli) {
        return prepareExpressions(keys, deli, "=");
    }

    default String prepareExpressions(List<String> keys, String deli, String sign) {
        return join(keys.stream().map(
                key -> "`" + key + "`" + sign + "?"
        ).collect(Collectors.toList()), "", deli);
    }

    default <T extends Object> String prepareRowsOfValues(List<List<T>> rowsOfValues) {
        List<String> valueGroups = rowsOfValues.stream().map(
                values -> groupQuestionMarks(values.size())
        ).collect(Collectors.toList());
        return join(valueGroups, "", ",");
    }

    default String bracket(CharSequence content) {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("(").append(content.toString()).append(")").toString();
    }

    default <T extends Object> String join(List<String> list, String stringBrace, String deli) {
        StringBuilder builder = new StringBuilder();
        int size = list.size();
        for (int i = 0; i < size; i++) {
            String item = list.get(i);
            builder.append(stringBrace + item + stringBrace);
            if (i < size - 1) {
                builder.append(deli);
            }
        }
        return builder.toString();
    }

    default String groupQuestionMarks(int count) {
        if (count == 0)
            return bracket("");
        StringBuilder builder = new StringBuilder("?");
        for (int i = 1; i < count; i++) {
            builder.append(",?");
        }
        return bracket(builder.toString());
    }

    default List<String> selectColumnNames(List<String> columnNames, List<Boolean> bitmap) {
        if (bitmap == null) {
            return columnNames;
        }
        ArrayList<String> namesSelected = new ArrayList();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i < bitmap.size()) {
                if (bitmap.get(i)) {
                    namesSelected.add(columnNames.get(i));
                }
            }
        }
        return namesSelected;
    }
}
