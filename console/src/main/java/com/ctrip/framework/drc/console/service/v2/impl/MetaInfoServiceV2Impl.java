package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.dao.v2.RegionTblDao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.DEFAULT_REPLICATOR_APPLIER_PORT;

/**
 * Created by yongnian
 * 2023/7/26 14:09
 */
@Service
public class MetaInfoServiceV2Impl implements MetaInfoServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Supplier<List<DcDo>> dcCache = Suppliers.memoizeWithExpiration(this::queryAllDc, 10, TimeUnit.SECONDS);

    private final Supplier<List<RegionTbl>> regionCache = Suppliers.memoizeWithExpiration(this::queryAllRegion, 10, TimeUnit.SECONDS);

    private final Supplier<List<BuTbl>> buCache = Suppliers.memoizeWithExpiration(this::queryAllBu, 10, TimeUnit.SECONDS);

    @Autowired
    private DcTblDao dcTblDao;

    @Autowired
    private BuTblDao buTblDao;

    @Autowired
    private RegionTblDao regionTblDao;

    @Autowired
    private ResourceTblDao resourceTblDao;

    @Autowired
    private ReplicatorTblDao replicatorTblDao;

    @Autowired
    private DefaultConsoleConfig consoleConfig;


    @Override
    public List<DcDo> queryAllDcWithCache() {
        return dcCache.get();
    }

    @Override
    public List<RegionTbl> queryAllRegionWithCache() {
        return regionCache.get();
    }

    @Override
    public List<BuTbl> queryAllBuWithCache() {
        return buCache.get();
    }

    @Override
    public List<BuTbl> queryAllBu() {
        try {
            return buTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllBu exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    @Override
    public List<RegionTbl> queryAllRegion() {
        try {
            return regionTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    @Override
    public List<DcDo> queryAllDc() {
        try {
            logger.info("queryAllDc");
            List<DcTbl> dcTbls = dcTblDao.queryAll();
            List<RegionTbl> regionTbls = regionTblDao.queryAll();
            Map<String, RegionTbl> regionTblMap = regionTbls.stream().collect(Collectors.toMap(RegionTbl::getRegionName, Function.identity()));

            return dcTbls.stream().map(e -> {
                DcDo dcDo = new DcDo();
                dcDo.setDcId(e.getId());
                dcDo.setDcName(e.getDcName());
                dcDo.setRegionName(e.getRegionName());

                RegionTbl regionTbl = regionTblMap.get(e.getRegionName());
                if (regionTbl != null) {
                    dcDo.setRegionId(regionTbl.getId());
                }
                return dcDo;
            }).collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    public Integer findAvailableApplierPort(String ip) throws Exception {
        ResourceTbl resourceTbl = resourceTblDao.queryAll().stream().filter(predicate -> (predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getIp().equalsIgnoreCase(ip))).findFirst().get();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAll().stream().filter(r -> r.getDeleted().equals(BooleanEnum.FALSE.getCode()) && r.getResourceId().equals(resourceTbl.getId())).collect(Collectors.toList());
        if(replicatorTbls.size() == 0) {
            return DEFAULT_REPLICATOR_APPLIER_PORT;
        }
        int size = consoleConfig.getAvailablePortSize();
        boolean[] isUsedFlags = new boolean[size];
        for (ReplicatorTbl r : replicatorTbls) {
            int index = r.getApplierPort() - DEFAULT_REPLICATOR_APPLIER_PORT;
            isUsedFlags[index] = true;
        }
        for (int i = 0; i <= size; i++) {
            if (!isUsedFlags[i]) {
                return DEFAULT_REPLICATOR_APPLIER_PORT + i;
            }
        }
        throw ConsoleExceptionUtils.message("no available port find for replicator, all in use!");
    }
}
