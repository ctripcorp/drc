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
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
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
    @Autowired
    private MysqlServiceV2 mysqlService;

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(6, "conflictLog"));

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

    @Override
    public ConflictTrxLogDetailView getConflictTrxLogDetailView(Long conflictTrxLogId) throws Exception {
        ConflictTrxLogDetailView view = new ConflictTrxLogDetailView();
        view.setConflictTrxLogId(conflictTrxLogId);

        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(conflictTrxLogId);
        if (conflictTrxLogTbl == null) {
            return view;
        }
        view.setTrxResult(conflictTrxLogTbl.getTrxResult());

        MhaTblV2 srcMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getSrcMhaName());
        MhaTblV2 dstMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getDstMhaName());
        DcTbl srcDcTbl = dcTblDao.queryById(srcMha.getDcId());
        DcTbl dstTbl = dcTblDao.queryById(dstMha.getDcId());
        view.setSrcDc(srcDcTbl.getDcName());
        view.setDstDc(dstTbl.getDcName());

        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }
        List<ConflictRowsLogDetailView> rowsLogDetailViews = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogDetailView target = new ConflictRowsLogDetailView();
            BeanUtils.copyProperties(source, target);
            return target;
        }).collect(Collectors.toList());
        view.setRowsLogDetailViews(rowsLogDetailViews);
        return view;
    }

    @Override
    public ConflictCurrentRecordView getConflictCurrentRecordView(Long conflictTrxLogId) throws Exception {
        ConflictCurrentRecordView view = new ConflictCurrentRecordView();
        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(conflictTrxLogId);
        if (conflictTrxLogTbl == null) {
            return view;
        }
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }
        List<String> rawSqlList = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getRawSql).collect(Collectors.toList());
        MhaTblV2 srcMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getSrcMhaName());
        MhaTblV2 dstMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getDstMhaName());

        List<Map<String, Object>> srcResults = new ArrayList<>();
        List<Map<String, Object>> dstResults = new ArrayList<>();
        List<ListenableFuture<Map<String, Object>>> srcFutures = new ArrayList<>();
        List<ListenableFuture<Map<String, Object>>> dstFutures = new ArrayList<>();
        for (String rawSql : rawSqlList) {
            ListenableFuture<Map<String, Object>> srcFuture = executorService.submit(() -> mysqlService.queryTableRecords(srcMha.getMhaName(), rawSql));
            ListenableFuture<Map<String, Object>> dstFuture = executorService.submit(() -> mysqlService.queryTableRecords(dstMha.getMhaName(), rawSql));
            srcFutures.add(srcFuture);
            dstFutures.add(dstFuture);
        }

        for (ListenableFuture<Map<String, Object>> future : srcFutures) {
            try {
                Map<String, Object> result = future.get(5, TimeUnit.SECONDS);
                srcResults.add(result);
            } catch (Exception e) {
                logger.error("query src records error: {}", e);
                throw ConsoleExceptionUtils.message("query src records error");
            }
        }
        for (ListenableFuture<Map<String, Object>> future : dstFutures) {
            try {
                Map<String, Object> result = future.get(5, TimeUnit.SECONDS);
                dstResults.add(result);
            } catch (Exception e) {
                logger.error("query dst records error: {}", e);
                throw ConsoleExceptionUtils.message("query dst records error");
            }
        }

        Map<String, List<Map<String, Object>>> srcResultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> dstResultMap = new HashMap<>();
        Map<String, List<String>> srcColumnMap = new HashMap<>();
        Map<String, List<String>> dstColumnMap = new HashMap<>();

        for (Map<String, Object> srcResult : srcResults) {
            String tableName = String.valueOf(srcResult.get("tableName"));
            List<Map<String, Object>> recordList = (List<Map<String, Object>>) srcResult.get("record");
            if (srcResultMap.containsKey(tableName)) {
                srcResultMap.get(tableName).addAll(recordList);
            } else {
                srcResultMap.put(tableName, recordList);
                List<String> columns = (List<String>) srcResult.get("columnList");
                srcColumnMap.put(tableName, columns);
            }
        }

        for (Map<String, Object> dstResult : dstResults) {
            String tableName = String.valueOf(dstResult.get("tableName"));
            List<Map<String, Object>> recordList = (List<Map<String, Object>>) dstResult.get("record");
            if (dstResultMap.containsKey(tableName)) {
                dstResultMap.get(tableName).addAll(recordList);
            } else {
                dstResultMap.put(tableName, recordList);
                List<String> columns = (List<String>) dstResult.get("columnList");
                dstColumnMap.put(tableName, columns);
            }
        }

        List<Map<String, Object>> srcRecords = new ArrayList<>();
        List<Map<String, Object>> dstRecords = new ArrayList<>();
        srcResultMap.forEach((tableName, records) -> {
            List<String> columns = srcColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            srcRecords.add(resultMap);
        });
        dstResultMap.forEach((tableName, records) -> {
            List<String> columns = dstColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            dstRecords.add(resultMap);
        });
        view.setSrcRecords(srcRecords);
        view.setDstRecords(dstRecords);
        return view;
    }
}
