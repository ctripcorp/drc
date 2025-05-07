package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.DlockTbl;
import com.ctrip.framework.drc.console.dao.v2.DlockTblDao;
import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.service.v2.DLockService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/21 15:57
 */
@Service
public class DlockServiceImpl implements DLockService {
    private final Logger logger = LoggerFactory.getLogger("autoConfig");

    private static final int LOCK_EXPIRE_MINUTES = 5;

    @Autowired
    private DlockTblDao dlockTblDao;

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void  tryLocks(List<String> mhaNames, DlockEnum dlock) throws ConsoleException {
        try {
            mhaNames = mhaNames.stream().distinct().toList();
            List<DlockTbl> tbls = buildDlockTbls(mhaNames, dlock.getLockName());
            dlockTblDao.deleteByMhaNames(tbls, LOCK_EXPIRE_MINUTES);
            dlockTblDao.batchInsert(tbls);
        } catch (SQLException e) {
            logger.error("[[tag=autoconfig]] get lock fail, mhaNames: {}, lockName: {}", mhaNames, dlock.getLockName(), e);
            throw ConsoleExceptionUtils.message("get lock fail, try again");
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void unlockLocks(List<String> mhaNames, DlockEnum dlock) throws ConsoleException {
        try {
            mhaNames = mhaNames.stream().distinct().toList();
            List<DlockTbl> tbls = buildDlockTbls(mhaNames, dlock.getLockName());
            dlockTblDao.deleteByMhaNames(tbls);
        } catch (SQLException e) {
            logger.error("[[tag=autoconfig]] unlock lock fail, mhaNames: {}, lockName: {}", mhaNames, dlock.getLockName(), e);
            throw ConsoleExceptionUtils.message("release lock fail");
        }

    }

    private List<DlockTbl> buildDlockTbls(List<String> mhaNames, String lockName) {
        List<DlockTbl> tbls = Lists.newArrayList();
        mhaNames.forEach(mhaName -> {
            DlockTbl dlockTbl = new DlockTbl();
            dlockTbl.setMhaName(mhaName);
            dlockTbl.setLockName(lockName);
            tbls.add(dlockTbl);
        });
        return tbls;
    }
}
