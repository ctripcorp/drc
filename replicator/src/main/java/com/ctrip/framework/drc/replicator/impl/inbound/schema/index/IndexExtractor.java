package com.ctrip.framework.drc.replicator.impl.inbound.schema.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/7/2
 */
public class IndexExtractor {

    private static final Logger logger = LoggerFactory.getLogger(IndexExtractor.class);

    public static final String PRIMARY = "PRIMARY";

    public static List<List<String>> extractIndex(ResultSet resultSet) {

        List<List<String>> identifies = Lists.newArrayList();

        try {
            Map<String, List<String>> uniqueIndexes = Maps.newHashMap();
            while (resultSet.next()) {
                String indexName = resultSet.getString(1);
                List<String> columnNames = uniqueIndexes.get(indexName);
                if (columnNames == null) {
                    columnNames = Lists.newArrayList();
                    uniqueIndexes.put(indexName, columnNames);
                }
                columnNames.add(resultSet.getString(2));
            }

            for (Map.Entry<String, List<String>> entry : uniqueIndexes.entrySet()) {
                if (PRIMARY.equalsIgnoreCase(entry.getKey())) {
                    identifies.add(0, entry.getValue());
                } else {
                    identifies.add(entry.getValue());
                }
            }
        } catch (Throwable t) {
            logger.error("IndexExtractor error", t);
        }

        return identifies;
    }
}
