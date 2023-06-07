package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/1 14:30
 */
@Service
public class TestService {

    @Autowired
    private DcTblDao dcTblDao;

    public List<DcTbl> getAll() throws SQLException {
        return dcTblDao.queryAll();
    }
}
