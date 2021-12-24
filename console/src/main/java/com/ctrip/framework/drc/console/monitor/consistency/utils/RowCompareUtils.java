package com.ctrip.framework.drc.console.monitor.consistency.utils;

import com.ctrip.framework.drc.console.monitor.consistency.cases.Row;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/11/20 下午8:18.
 */
public class RowCompareUtils {

    protected static Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    public static Map<String, Row> difference(Map<String, Row> src, Map<String, Row> dst) {

        Map<String, Row> diffRow = Maps.newHashMap();

        if (src == null && dst == null) {
            return diffRow;
        }

        if (src == null) {
            return dst;
        }

        if (dst == null) {
            return src;
        }

        Set<String> dstKeys = Sets.newHashSet(dst.keySet());

        for (String key : dstKeys) {
            Row dstRow = dst.remove(key);
            Row srcRow = src.remove(key);

            if (srcRow == null) {
                diffRow.put(key, dstRow);
                logger.info("[difference] with key {}, srcRow is null", key);
            } else {
                if (!dstRow.equals(srcRow)) {
                    diffRow.put(key, dstRow);
                    logger.info("[difference] with key {}, not equal", key);
                }
            }
        }

        if (!src.isEmpty()) {
            for (String key : src.keySet()) {
                logger.info("[difference] with key {}, dstRow is null", key);
            }
            diffRow.putAll(src);
        }

        logger.info("[difference] size {} from dstKeys size {} ", diffRow.size(), dstKeys.size());
        return diffRow;
    }
}
