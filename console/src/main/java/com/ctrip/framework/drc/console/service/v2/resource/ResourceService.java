package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.param.v2.resource.*;
import com.ctrip.framework.drc.console.vo.v2.*;
import com.ctrip.framework.drc.core.entity.Applier;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/3 16:10
 */
public interface ResourceService {

    void configureResource(ResourceBuildParam param) throws Exception;

    void batchConfigureResource(ResourceBuildParam param) throws Exception;

    void offlineResource(long resourceId) throws Exception;

    void onlineResource(long resourceId) throws Exception;

    void deactivateResource(long resourceId) throws Exception;

    void recoverResource(long resourceId) throws Exception;

    List<ResourceView> getResourceView(ResourceQueryParam param) throws Exception;

    /**
     * same dc
     * same tag
     * same type
     */
    List<ResourceView> getResourceViewByIp(String ip) throws Exception;

    List<ResourceView> getMhaAvailableResource(String mhaName, int type) throws SQLException;

    List<ResourceView> getMhaDbAvailableResource(String mhaName, int type) throws SQLException;


    List<ResourceView> getMhaDbAvailableResourceWithUse(String srcMhaName, String dstMhaName, int type) throws Exception;

    List<ResourceView> getMhaAvailableResourceWithUse(String mhaName, int type) throws Exception;
    
    List<ResourceView> autoConfigureResource(ResourceSelectParam param) throws SQLException;
    List<ResourceView> autoConfigureMhaDbResource(DbResourceSelectParam param) throws SQLException;

    List<ResourceView> handOffResource(ResourceSelectParam param) throws SQLException;

    List<ResourceView> handOffResource(List<String> selectedIps, List<ResourceView> availableResource);

    List<MhaView> queryMhaByReplicator(long resourceId) throws Exception;

    List<ApplierReplicationView> queryReplicationByApplier(long resourceId) throws Exception;

    int migrateResource(String newIp, String oldIp, int type) throws Exception;

    int partialMigrateReplicator(ReplicatorMigrateParam param) throws Exception;

    int partialMigrateApplier(ApplierMigrateParam param) throws Exception;

    int migrateSlaveReplicator(String newIp, String oldIp) throws Exception;

    void migrateResource(ResourceMigrateParam param) throws Exception;

    ResourceSameAzView checkResourceAz() throws Exception;

}
