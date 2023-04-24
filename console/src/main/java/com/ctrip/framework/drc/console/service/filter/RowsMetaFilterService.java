package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/24 10:18
 */
public interface RowsMetaFilterService {

    List<String> getWhiteList(String metaFilterName) throws SQLException;

    boolean addWhiteList(RowsMetaFilterParam requestBody);

    boolean deleteWhiteList (RowsMetaFilterParam requestBody);

    /**
     * full override
     * @param requestBody
     * @return
     */
    boolean updateWhiteList(RowsMetaFilterParam requestBody);
}
