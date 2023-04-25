package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/4/24 10:18
 */
public interface RowsMetaFilterService {

    QConfigDataVO getWhiteList(String metaFilterName) throws SQLException;

    boolean addWhiteList(RowsMetaFilterParam param, String operator) throws SQLException;

    boolean deleteWhiteList (RowsMetaFilterParam param, String operator) throws SQLException;

    /**
     * full override
     * @param param
     * @return
     */
    boolean updateWhiteList(RowsMetaFilterParam param, String operator) throws SQLException;
}
