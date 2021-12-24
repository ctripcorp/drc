package com.ctrip.framework.drc.console.common.bean;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.platform.dal.dao.DalClient;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.DalRowMapper;
import com.ctrip.platform.dal.dao.helper.DalClientFactoryListener;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaMapper;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

/**
 * @author wangjixin
 * @version 1.0
 * date: 2020-02-25
 */
@Configuration
public class DalConfig {

    public static final String DRC_TITAN_KEY = "fxdrcmetadb_w";

    @Bean
    public ApplierUploadLogTblDao applierUploadLogTblDao() throws SQLException {
        return new ApplierUploadLogTblDao();
    }

    @Bean
    public ConflictLogDao conflictLogDao() throws SQLException {
        return new ConflictLogDao();
    }

    @Bean
    public BuTblDao buTblDao() throws SQLException {
        return new BuTblDao();
    }

    @Bean
    public ClusterMhaMapTblDao clusterMhaMapTblDao() throws SQLException {
        return new ClusterMhaMapTblDao();
    }

    @Bean
    public DcTblDao dcTblDao() throws SQLException {
        return new DcTblDao();
    }

    @Bean
    public ClusterTblDao clusterTblDao() throws SQLException {
        return new ClusterTblDao();
    }

    @Bean
    public MhaGroupTblDao mhaGroupTblDao() throws SQLException {
        return new MhaGroupTblDao();
    }

    @Bean
    public MhaTblDao mhaTblDao() throws SQLException {
        return new MhaTblDao();
    }

    @Bean
    public DbTblDao dbTblDao() throws SQLException {
        return new DbTblDao();
    }

    @Bean
    public ResourceTblDao resourceTblDao() throws SQLException {
        return new ResourceTblDao();
    }

    @Bean
    public MachineTblDao machineTblDao() throws SQLException {
        return new MachineTblDao();
    }

    @Bean
    public ReplicatorGroupTblDao replicatorGroupTblDao() throws SQLException {
        return new ReplicatorGroupTblDao();
    }

    @Bean
    public ApplierGroupTblDao applierGroupTblDao() throws SQLException {
        return new ApplierGroupTblDao();
    }

    @Bean
    public ClusterManagerTblDao clusterManagerTblDao() throws SQLException {
        return new ClusterManagerTblDao();
    }

    @Bean
    public ZookeeperTblDao zookeeperTblDao() throws SQLException {
        return new ZookeeperTblDao();
    }

    @Bean
    public ReplicatorTblDao replicatorTblDao() throws SQLException {
        return new ReplicatorTblDao();
    }

    @Bean
    public ApplierTblDao applierTblDao() throws SQLException {
        return new ApplierTblDao();
    }

    @Bean
    public DdlHistoryTblDao ddlHistoryTblDao() throws SQLException {
        return new DdlHistoryTblDao();
    }

    @Bean
    public DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao() throws SQLException {
        return new DataConsistencyMonitorTblDao();
    }

    @Bean
    public DalQueryDao dalQueryDao() {
        return new DalQueryDao(DRC_TITAN_KEY);
    }

    @Bean
    public DalClient dalClient() {
        return DalClientFactory.getClient(DRC_TITAN_KEY);
    }

    @Bean
    public ServletListenerRegistrationBean dalListener() {
        return new ServletListenerRegistrationBean<>(new DalClientFactoryListener());
    }

    @Bean
    public DalRowMapper<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTblMapper() throws SQLException {
        return  new DalDefaultJpaMapper<>(DataInconsistencyHistoryTbl.class);
    }

}
