package com.ctrip.framework.drc.console.service.v2.impl;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigService;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class CommonDataInit {
    @Mock
    TransactionMonitor transactionMonitor;
    @Mock
    RowsFilterServiceV2 rowsFilterService;
    @Mock
    DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    DbReplicationTblDao dbReplicationTblDao;
    @Mock
    MessengerTblDao messengerTblDao;
    @Mock
    ResourceTblDao resourceTblDao;
    @Mock
    DbTblDao dbTblDao;
    @Mock
    MessengerFilterTblDao messengerFilterTblDao;
    @Mock
    MhaTblV2Dao mhaTblV2Dao;
    @Mock
    DefaultConsoleConfig defaultConsoleConfig;
    @Mock
    DomainConfig domainConfig;
    @Mock
    QConfigService qConfigService;
    @Mock
    DefaultConsoleConfig consoleConfig;
    @Mock
    MetaInfoServiceV2 metaInfoServiceV2;
    @Mock
    DrcBuildService drcBuildService;
    @Mock
    AbstractDao abstractDao;
    @Mock
    MysqlServiceV2 mysqlServiceV2;
    @Mock
    DcTblDao dcTblDao;
    @Mock
    BuTblDao buTblDao;
    @Mock
    RegionTblDao regionTblDao;
    @Mock
    MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    MachineTblDao machineTblDao;
    @Mock
    ReplicatorTblDao replicatorTblDao;
    @Mock
    ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Mock
    ApplierTblV2Dao applierTblV2Dao;
    @Mock
    MessengerServiceV2 messengerService;
    @InjectMocks
    MetaInfoServiceV2Impl metaInfoServiceV2Impl;
    @InjectMocks
    MessengerServiceV2Impl messengerServiceV2Impl;
    @InjectMocks
    MhaReplicationServiceV2Impl mhaReplicationServiceV2;

    @Mock
    ColumnsFilterServiceV2 columnsFilterServiceV2;
    RowsFilterServiceV2 rowsFilterServiceV2;
    @Mock
    RouteTblDao routeTblDao;
    @Mock
    ProxyTblDao proxyTblDao;
    @Mock
    ClusterManagerTblDao clusterManagerTblDao;
    @Mock
    ZookeeperTblDao zookeeperTblDao;
    @Mock
    RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Mock
    ColumnsFilterTblV2Dao columnsFilterTblV2Dao;


    @Before
    public void setUp() throws SQLException, IOException {
        // ApplierTblV2
        List<ApplierTblV2> applierTblV2s = this.getData("ApplierTblV2.json", ApplierTblV2.class);
        when(applierTblV2Dao.queryByApplierGroupId(anyLong(), anyInt())).thenAnswer(i -> {
            Long applierGroupID = i.getArgument(0, Long.class);
            Integer deleted = i.getArgument(1, Integer.class);

            return applierTblV2s.stream()
                    .filter(e -> applierGroupID.equals(e.getApplierGroupId()) && deleted.equals(e.getDeleted()))
                    .collect(Collectors.toList());
        });


        // ApplierGroupTblV2
        List<ApplierGroupTblV2> applierGroupTblV2s = this.getData("ApplierGroupTblV2.json", ApplierGroupTblV2.class);
        when(applierGroupTblV2Dao.queryByMhaReplicationId(anyLong(), anyInt())).thenAnswer(i -> {
            Long mhaReplicationId = i.getArgument(0, Long.class);
            Integer deleted = i.getArgument(1, Integer.class);

            return applierGroupTblV2s.stream()
                    .filter(e -> mhaReplicationId.equals(e.getMhaReplicationId()) && deleted.equals(e.getDeleted()))
                    .findFirst().orElse(null);
        });

        // ResourceTbl
        List<ResourceTbl> resourceTbls = this.getData("ResourceTbl.json", ResourceTbl.class);
        when(resourceTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List ids = i.getArgument(0, List.class);
            return resourceTbls.stream().filter(e -> ids.contains(e.getId())).collect(Collectors.toList());
        });
        // ReplicatorTbl
        List<ReplicatorTbl> replicatorTbls = this.getData("ReplicatorTbl.json", ReplicatorTbl.class);
        when(replicatorTblDao.queryByRGroupIds(anyList(), anyInt())).thenAnswer(i -> {
            List replicatorGroupId = i.getArgument(0, List.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return replicatorTbls.stream().filter(e -> replicatorGroupId.contains(e.getRelicatorGroupId()) && e.getDeleted().equals(deleted)).collect(Collectors.toList());
        });
        // ReplicatorGroupTbl
        List<ReplicatorGroupTbl> replicatorGroupTbls = this.getData("ReplicatorGroupTbl.json", ReplicatorGroupTbl.class);
        when(replicatorGroupTblDao.queryByMhaId(anyLong())).thenAnswer(i -> {
            Long mhaId = i.getArgument(0, Long.class);
            return replicatorGroupTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).findFirst().orElse(null);
        });

        // MachineTbl
        List<MachineTbl> machineTbls = this.getData("MachineTbl.json", MachineTbl.class);
        when(machineTblDao.queryByMhaId(anyLong(), anyInt())).thenAnswer(i -> {
            Long mhaId = i.getArgument(0, Long.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return machineTbls.stream().filter(e -> e.getMhaId().equals(mhaId) && e.getDeleted().equals(deleted)).collect(Collectors.toList());
        });

        // BuTbl
        List<BuTbl> buTbls = this.getData("BuTbl.json", BuTbl.class);
        when(buTblDao.queryAll()).thenReturn(buTbls);


        // RegionTbl
        List<RegionTbl> regionTbls = this.getData("RegionTbl.json", RegionTbl.class);
        when(regionTblDao.queryAll()).thenReturn(regionTbls);


        // DcTbl
        List<DcTbl> dcTbls = this.getData("DcTbl.json", DcTbl.class);
        when(dcTblDao.queryAll()).thenReturn(dcTbls);

        // MhaReplicationTbl
        List<MhaReplicationTbl> mhaReplicationTbls = this.getData("MhaReplicationTbl.json", MhaReplicationTbl.class);
        when(mhaReplicationTblDao.queryById(anyLong())).thenAnswer(i -> {
            Long id = i.getArgument(0, Long.class);
            return mhaReplicationTbls.stream().filter(e -> id.equals(e.getId())).findFirst().orElse(null);
        });
        when(mhaReplicationTblDao.queryByMhaId(anyLong(), anyLong(), anyInt())).thenAnswer(i -> {
            Long srcMhaId = i.getArgument(0, Long.class);
            Long dstMhaId = i.getArgument(1, Long.class);
            Integer deleted = i.getArgument(2, Integer.class);
            return mhaReplicationTbls.stream().filter(e -> e.getSrcMhaId().equals(srcMhaId) && e.getDstMhaId().equals(dstMhaId) && Objects.equals(e.getDeleted(), deleted)).findFirst().orElse(null);
        });
        when(mhaReplicationTblDao.queryByMhaId(anyLong(), anyLong())).thenAnswer(i -> {
            Long srcMhaId = i.getArgument(0, Long.class);
            Long dstMhaId = i.getArgument(1, Long.class);
            return mhaReplicationTbls.stream().filter(e -> e.getSrcMhaId().equals(srcMhaId) && e.getDstMhaId().equals(dstMhaId)).findFirst().orElse(null);
        });
        when(mhaReplicationTblDao.queryByPage(any(MhaReplicationQuery.class))).thenAnswer(i -> {
            MhaReplicationQuery query = i.getArgument(0, MhaReplicationQuery.class);
            return mhaReplicationTbls.stream().filter(e -> {
                        if (!CollectionUtils.isEmpty(query.getSrcMhaIdList()) && !query.getSrcMhaIdList().contains(e.getSrcMhaId())) {
                            return false;
                        }
                        return CollectionUtils.isEmpty(query.getDstMhaIdList()) || query.getDstMhaIdList().contains(e.getSrcMhaId());
                    }).sorted(Comparator.comparing(MhaReplicationTbl::getId).thenComparing(Comparator.comparing(MhaReplicationTbl::getDatachangeLasttime).reversed()))
                    .skip((long) (query.getPageIndex() - 1) * query.getPageSize())
                    .limit(query.getPageSize()).collect(Collectors.toList());
        });

        when(mhaReplicationTblDao.count(any(MhaReplicationQuery.class))).thenAnswer(i -> {
            MhaReplicationQuery query = i.getArgument(0, MhaReplicationQuery.class);
            return (int) mhaReplicationTbls.stream().filter(e -> {
                if (!CollectionUtils.isEmpty(query.getSrcMhaIdList()) && !query.getSrcMhaIdList().contains(e.getSrcMhaId())) {
                    return false;
                }
                return CollectionUtils.isEmpty(query.getDstMhaIdList()) || query.getDstMhaIdList().contains(e.getSrcMhaId());
            }).count();
        });


        when(mhaReplicationTblDao.queryByRelatedMhaId(anyList())).thenAnswer(i -> {
            List<Long> relatedMhaList = i.getArgument(0, List.class);
            return mhaReplicationTbls.stream().filter(e -> {
                if (CollectionUtils.isEmpty(relatedMhaList)) {
                    return false;
                }
                return relatedMhaList.contains(e.getSrcMhaId()) || relatedMhaList.contains(e.getDstMhaId());
            }).collect(Collectors.toList());
        });



        // messengerGroupTbl
        List<MessengerGroupTbl> messengerGroupTbls = this.getData("MessengerGroupTbl.json", MessengerGroupTbl.class);
        when(messengerGroupTblDao.queryBy(any())).thenReturn(messengerGroupTbls);
        when(messengerGroupTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List ids = i.getArgument(0, List.class);
            return messengerGroupTbls.stream().filter(e -> ids.contains(e.getId())).collect(Collectors.toList());
        });
        when(messengerGroupTblDao.queryByMhaId(anyLong(), anyInt())).thenAnswer(i -> {
            Long mhaId = i.getArgument(0, Long.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return messengerGroupTbls.stream().filter(e -> e.getMhaId().equals(mhaId) && e.getDeleted().equals(deleted)).findFirst().orElse(null);
        });
        when(messengerGroupTblDao.queryByPk(anyLong())).thenAnswer(i -> {
            Long id = i.getArgument(0, Long.class);
            return messengerGroupTbls.stream().filter(e -> e.getId().equals(id)).collect(Collectors.toList());
        });

        // messengerGroupTbl
        List<MessengerTbl> messengerTbls = this.getData("MessengerTbl.json", MessengerTbl.class);
        when(messengerTblDao.queryByGroupId(anyLong())).thenAnswer(i -> {
            Long groupId = i.getArgument(0, Long.class);
            return messengerTbls.stream().filter(e -> groupId.equals(e.getMessengerGroupId())).collect(Collectors.toList());
        });


        // MhaTblV2
        List<MhaTblV2> mhaTblV2List = this.getData("MhaTbl.json", MhaTblV2.class);
        when(mhaTblV2Dao.queryByIds(any())).thenAnswer(i -> {
            List ids = i.getArgument(0, List.class);
            return mhaTblV2List.stream().filter(e -> ids.contains(e.getId())).collect(Collectors.toList());
        });
        when(mhaTblV2Dao.queryByMhaName(anyString(), anyInt())).thenAnswer(i -> {
            String mhaName = i.getArgument(0, String.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return mhaTblV2List.stream().filter(e -> mhaName.equals(e.getMhaName()) && deleted.equals(e.getDeleted())).findFirst().orElse(null);
        });
        when(mhaTblV2Dao.queryByMhaNames(anyList(), anyInt())).thenAnswer(i -> {
            List<String> mhaNames = i.getArgument(0, List.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return mhaTblV2List.stream().filter(e -> mhaNames.contains(e.getMhaName()) && deleted.equals(e.getDeleted()))
                    .collect(Collectors.toList());
        });



        // MhaDbMappingTbl
        List<MhaDbMappingTbl> mhaDbMappingTbls = this.getData("MhaDbMappingTbl.json", MhaDbMappingTbl.class);
        when(mhaDbMappingTblDao.queryByMhaId(anyLong())).thenAnswer(i -> {
            Long mhaId = i.getArgument(0, Long.class);
            return mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).collect(Collectors.toList());
        });
        when(mhaDbMappingTblDao.queryByMhaIds(anyList())).thenAnswer(i -> {
            List mhaIds = i.getArgument(0, List.class);
            return mhaDbMappingTbls.stream().filter(e -> mhaIds.contains(e.getMhaId())).collect(Collectors.toList());
        });
        when(mhaDbMappingTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List mhaDbMappingIds = i.getArgument(0, List.class);
            return mhaDbMappingTbls.stream().filter(e -> mhaDbMappingIds.contains(e.getId())).collect(Collectors.toList());
        });
        when(mhaDbMappingTblDao.queryById(anyLong())).thenAnswer(i -> {
            Long mhaDbMappingId = i.getArgument(0, Long.class);
            return mhaDbMappingTbls.stream().filter(e -> mhaDbMappingId.equals(e.getId())).findFirst().orElse(null);
        });
        when(mhaDbMappingTblDao.queryByDbIdsAndMhaId(anyList(), anyLong())).thenAnswer(i -> {
            List<Long> dbIds = i.getArgument(0, List.class);
            Long mhaId = i.getArgument(1, Long.class);
            return mhaDbMappingTbls.stream().filter(e -> {
                return dbIds.contains(e.getDbId()) && mhaId.equals(e.getMhaId());
            }).collect(Collectors.toList());
        });


        // DbReplicationTbl
        List<DbReplicationTbl> dbReplicationTbls = this.getData("DbReplicationTbl.json", DbReplicationTbl.class);
        when(dbReplicationTblDao.queryBySrcMappingIds(anyList(), anyInt())).thenAnswer(i -> {
            List srcMhaDbMappingIds = i.getArgument(0, List.class);
            Integer replicationType = i.getArgument(1, Integer.class);
            return dbReplicationTbls.stream().filter(e -> srcMhaDbMappingIds.contains(e.getSrcMhaDbMappingId()) && replicationType.equals(e.getReplicationType()))
                    .collect(Collectors.toList());
        });
        when(dbReplicationTblDao.queryByDestMappingIds(anyList(), anyInt())).thenAnswer(i -> {
            List dstMhaDbMappingIds = i.getArgument(0, List.class);
            Integer replicationType = i.getArgument(1, Integer.class);
            return dbReplicationTbls.stream().filter(e -> dstMhaDbMappingIds.contains(e.getDstMhaDbMappingId()) && replicationType.equals(e.getReplicationType()))
                    .collect(Collectors.toList());
        });
        when(dbReplicationTblDao.queryByMappingIds(anyList(), anyList(), anyInt())).thenAnswer(i -> {
            List srcMhaDbMappingIds = i.getArgument(0, List.class);
            List dstMhaDbMappingIds = i.getArgument(1, List.class);
            Integer replicationType = i.getArgument(2, Integer.class);
            return dbReplicationTbls.stream()
                    .filter(e ->
                            srcMhaDbMappingIds.contains(e.getSrcMhaDbMappingId())
                                    && dstMhaDbMappingIds.contains(e.getDstMhaDbMappingId())
                                    && replicationType.equals(e.getReplicationType()))
                    .collect(Collectors.toList());
        });

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            List<DbReplicationTbl> dbReplicationTblList = (List<DbReplicationTbl>) args[0];
            for (int i = 0; i < dbReplicationTblList.size(); i++) {
                dbReplicationTblList.get(i).setId((long) (i + 1));
            }
            return null; // void method, so return null
        }).when(dbReplicationTblDao).batchInsertWithReturnId(any());

        when(abstractDao.batchUpdate(any())).thenAnswer(i -> {
            List list = i.getArgument(0, List.class);
            if (CollectionUtils.isEmpty(list)) {
                return new int[0];
            }
            Class<?> aClass = list.get(0).getClass();
            if (aClass.equals(DbReplicationTbl.class)) {
                List<DbReplicationTbl> typeList = list;
                return typeList.stream().mapToInt(e -> e.getId().intValue()).toArray();
            }
            if (aClass.equals(DbReplicationFilterMappingTbl.class)) {
                List<DbReplicationFilterMappingTbl> typeList = list;
                return typeList.stream().mapToInt(e -> e.getId().intValue()).toArray();
            }
            throw new IllegalStateException("Unexpected value: " + aClass);
        });

        when(abstractDao.insertWithReturnId(any())).thenAnswer(i -> {
            Object tbl = i.getArgument(0, List.class);
            if (tbl == null) {
                return 0;
            }
            Class<?> aClass = tbl.getClass();
            return 1;
        });

        when(dbReplicationTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List dbReplicationIds = i.getArgument(0, List.class);
            return dbReplicationTbls.stream().filter(e -> dbReplicationIds.contains(e.getId()))
                    .collect(Collectors.toList());
        });

        when(dbReplicationTblDao.queryById(anyLong())).thenAnswer(i -> {
            Long dbReplicationId = i.getArgument(0, Long.class);
            return dbReplicationTbls.stream().filter(e -> dbReplicationId.equals(e.getId()))
                    .findFirst().orElse(null);
        });

        // DbReplicationFilterMappingTbl
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = this.getData("DbReplicationFilterMappingTbl.json", DbReplicationFilterMappingTbl.class);
        when(dbReplicationFilterMappingTblDao.queryMessengerDbReplicationByIds(anyList())).thenAnswer(i -> {
            List dbReplicationIds = i.getArgument(0, List.class);
            return dbReplicationFilterMappingTbls.stream().filter(e -> dbReplicationIds.contains(e.getDbReplicationId()))
                    .collect(Collectors.toList());
        });
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(anyList())).thenAnswer(i -> {
            List dbReplicationIds = i.getArgument(0, List.class);
            return dbReplicationFilterMappingTbls.stream().filter(e -> dbReplicationIds.contains(e.getDbReplicationId()))
                    .collect(Collectors.toList());
        });
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(anyLong())).thenAnswer(i -> {
            Long dbReplicationId = i.getArgument(0, Long.class);
            return dbReplicationFilterMappingTbls.stream().filter(e -> dbReplicationId.equals(e.getDbReplicationId()))
                    .collect(Collectors.toList());
        });


        // DbTbl
        List<DbTbl> dbTbls = this.getData("DbTbl.json", DbTbl.class);
        when(dbTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List ids = i.getArgument(0, List.class);
            return dbTbls.stream()
                    .filter(e -> ids.contains(e.getId()))
                    .collect(Collectors.toList());
        });
        when(dbTblDao.queryById(anyLong())).thenAnswer(i -> {
            Long id = i.getArgument(0, Long.class);
            return dbTbls.stream()
                    .filter(e -> id.equals(e.getId()))
                    .findFirst().orElse(null);
        });
        when(dbTblDao.queryByDbNames(anyList())).thenAnswer(i -> {
            List names = i.getArgument(0, List.class);
            return dbTbls.stream()
                    .filter(e -> names.contains(e.getDbName()))
                    .collect(Collectors.toList());
        });

        // DbTbl
        List<MessengerFilterTbl> messengerFilterTbls = this.getData("MessengerFilterTbl.json", MessengerFilterTbl.class);
        when(messengerFilterTblDao.queryByIds(anyList())).thenAnswer(i -> {
            List ids = i.getArgument(0, List.class);
            return messengerFilterTbls.stream()
                    .filter(e -> ids.contains(e.getId()))
                    .collect(Collectors.toList());
        });
        when(messengerFilterTblDao.queryById(anyLong())).thenAnswer(i -> {
            Long ids = i.getArgument(0, Long.class);
            return messengerFilterTbls.stream()
                    .filter(e -> ids.equals(e.getId()))
                    .findFirst().orElse(null);
        });
        when(messengerFilterTblDao.queryAll()).thenReturn(messengerFilterTbls);

        // Dc
        List<DcDo> dcDoList = this.getData("DcDo.json", DcDo.class);
        when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(dcDoList);


        // abstractDao
        when(abstractDao.batchInsert(any())).thenAnswer(i -> {
            List list = i.getArgument(0, List.class);
            if (CollectionUtils.isEmpty(list)) {
                return new int[0];
            }
            Class<?> aClass = list.get(0).getClass();
            if (aClass.equals(DbTbl.class)) {
                List<DbTbl> typeList = list;
                dbTbls.addAll(typeList);
                return typeList.stream().mapToInt(e -> e.getId().intValue()).toArray();
            } else if (aClass.equals(MhaDbMappingTbl.class)) {
                List<MhaDbMappingTbl> typeList = list;
                mhaDbMappingTbls.addAll(typeList);
                return typeList.stream().mapToInt(e -> e.getId().intValue()).toArray();
            } else if (aClass.equals(DbReplicationFilterMappingTbl.class)) {
                List<DbReplicationFilterMappingTbl> typeList = list;
                dbReplicationFilterMappingTbls.addAll(typeList);
                return typeList.stream().mapToInt(e -> e.getId().intValue()).toArray();
            }
            throw new IllegalStateException("Unexpected value: " + aClass);
        });


        List<ClusterManagerTbl> clusterManagerTblList = this.getData("ClusterManagerTbl.json", ClusterManagerTbl.class);
        List<RouteTbl> routeTblList = this.getData("RouteTbl.json", RouteTbl.class);
        List<ProxyTbl> proxyTblList = this.getData("ProxyTbl.json", ProxyTbl.class);
        List<ZookeeperTbl> zookeeperTblList = this.getData("ZookeeperTbl.json", ZookeeperTbl.class);
        List<RowsFilterTblV2> rowsFilterTblV2List = this.getData("RowsFilterTblV2.json", RowsFilterTblV2.class);
        List<ColumnsFilterTblV2> columnsFilterTblV2List = this.getData("ColumnsFilterTblV2.json", ColumnsFilterTblV2.class);


        // queryALlExist
        when(applierTblV2Dao.queryAllExist()).thenReturn(applierTblV2s.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(applierGroupTblV2Dao.queryAllExist()).thenReturn(applierGroupTblV2s.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(messengerGroupTblDao.queryAllExist()).thenReturn(messengerGroupTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(messengerTblDao.queryAllExist()).thenReturn(messengerTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(dbReplicationTblDao.queryAllExist()).thenReturn(dbReplicationTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(mhaDbMappingTblDao.queryAllExist()).thenReturn(mhaDbMappingTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(mhaReplicationTblDao.queryAllExist()).thenReturn(mhaReplicationTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(mhaTblV2Dao.queryAllExist()).thenReturn(mhaTblV2List.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(dbTblDao.queryAllExist()).thenReturn(dbTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(buTblDao.queryAllExist()).thenReturn(buTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(dcTblDao.queryAllExist()).thenReturn(dcTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(resourceTblDao.queryAllExist()).thenReturn(resourceTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(machineTblDao.queryAllExist()).thenReturn(machineTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(replicatorGroupTblDao.queryAllExist()).thenReturn(replicatorGroupTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(replicatorTblDao.queryAllExist()).thenReturn(replicatorTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(messengerFilterTblDao.queryAllExist()).thenReturn(messengerFilterTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(dbReplicationFilterMappingTblDao.queryAllExist()).thenReturn(dbReplicationFilterMappingTbls.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));

        when(clusterManagerTblDao.queryAllExist()).thenReturn(clusterManagerTblList.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(routeTblDao.queryAllExist()).thenReturn(routeTblList.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(proxyTblDao.queryAllExist()).thenReturn(proxyTblList.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(zookeeperTblDao.queryAllExist()).thenReturn(zookeeperTblList.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(rowsFilterTblV2Dao.queryAllExist()).thenReturn(rowsFilterTblV2List.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));
        when(columnsFilterTblV2Dao.queryAllExist()).thenReturn(columnsFilterTblV2List.stream().filter(e -> !BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList()));

    }


    private <T> List<T> getData(String fileName, Class<T> clazz) throws IOException {
        String prefix = "/testData/messengerServiceV2/";
        String json = IOUtils.toString(Objects.requireNonNull(this.getClass().getResourceAsStream(prefix + fileName)), StandardCharsets.UTF_8);
        return JSON.parseArray(json, clazz);
    }

    @Test
    public void testGetData() throws IOException {
        List<MessengerGroupTbl> messengerGroupTbls = getData("MessengerGroupTbl.json", MessengerGroupTbl.class);
        assert messengerGroupTbls.size() > 0;
    }
}
