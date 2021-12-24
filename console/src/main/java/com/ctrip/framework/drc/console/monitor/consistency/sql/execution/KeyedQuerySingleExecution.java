package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:03.
 */
public class KeyedQuerySingleExecution extends AbstractNameAwareExecution implements NameAwareExecution {

    public static final String QUERY_SQL = "select * from %s where `%s` in (%s) order by %s limit %d";  //if exceed LIMIT, truncate

    private Set<String> keys;

    public KeyedQuerySingleExecution(TableNameDescriptor tableDescriptor, Set<String> keys) {
        super(tableDescriptor);
        this.keys = keys;
    }

    @Override
    protected String getQuerySql() {
        String table = getTable();
        String key = getKey();
        Set<Object> querySet = Sets.newHashSet();
        if (keys != null && keys.size() > PagedExecution.LIMIT) {
            logger.info("[Truncate] keys from {} to {}", keys.size(), PagedExecution.LIMIT);
            for (Object o : keys) {
                querySet.add(o);
                if (querySet.size() >= PagedExecution.LIMIT) {
                    break;
                }
            }
            logger.info("select {} keys {} to compare", PagedExecution.LIMIT, querySet);
        } else {
            querySet.addAll(keys);
        }
        String keySet = StringUtils.join(querySet, ",");
        return String.format(QUERY_SQL, table, key, keySet, key, PagedExecution.LIMIT);
    }

}
