package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogView;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/9/26 16:06
 */
@Service
public class ConflictLogServiceImpl implements ConflictLogService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Autowired
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DcTblDao dcTblDao;

    @Override
    public List<ConflictTrxLogView> getConflictTrxLogView(ConflictTrxLogQueryParam param) throws Exception {
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            return new ArrayList<>();
        }

        List<ConflictTrxLogView> views = conflictTrxLogTbls.stream().map(source -> {
            ConflictTrxLogView target = new ConflictTrxLogView();
            BeanUtils.copyProperties(source, target, "handleTime");
            target.setConflictTrxLogId(source.getId());
            target.setHandleTime(DateUtils.longToString(source.getHandleTime()));

            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public List<ConflictRowsLogView> getConflictRowsLogView(ConflictRowsLogQueryParam param) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return new ArrayList<>();
        }

        List<Long> conflictTrxLogIds = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getConflictTrxLogId).collect(Collectors.toList());
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByIds(conflictTrxLogIds);
        Set<String> mhaNames = new HashSet<>();
        List<String> srcMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getSrcMhaName).distinct().collect(Collectors.toList());
        List<String> dstMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getDstMhaName).distinct().collect(Collectors.toList());
        mhaNames.addAll(srcMhaNames);
        mhaNames.addAll(dstMhaNames);
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(mhaNames));
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();

        Map<Long, ConflictTrxLogTbl> conflictTrxLogMap = conflictTrxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getId, Function.identity()));
        Map<String, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, Function.identity()));
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        List<ConflictRowsLogView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogView target = new ConflictRowsLogView();
            BeanUtils.copyProperties(source, target, "handleTime");
            target.setHandleTime(DateUtils.longToString(source.getHandleTime()));
            target.setConflictRowsLogId(source.getId());

            ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogMap.get(source.getConflictTrxLogId());
            MhaTblV2 srcMha = mhaMap.get(conflictTrxLogTbl.getSrcMhaName());
            MhaTblV2 dstMha = mhaMap.get(conflictTrxLogTbl.getDstMhaName());
            target.setSrcDc(dcMap.get(srcMha.getDcId()));
            target.setDstDc(dcMap.get(dstMha.getDcId()));
            return target;
        }).collect(Collectors.toList());
        return views;
    }
}
