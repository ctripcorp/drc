package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;

/**
 * Created by dengquanliang
 * 2023/4/24 10:18
 */
public interface RowsFilterMetaService {

    QConfigDataVO getWhiteList(String metaFilterName) throws Exception;

    boolean addWhiteList(RowsMetaFilterParam param, String operator) throws Exception;

    boolean deleteWhiteList (RowsMetaFilterParam param, String operator) throws Exception;

    /**
     * full override
     * @param param
     * @return
     */
    boolean updateWhiteList(RowsMetaFilterParam param, String operator) throws Exception;
}
