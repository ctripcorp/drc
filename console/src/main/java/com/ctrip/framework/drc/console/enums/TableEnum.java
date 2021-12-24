package com.ctrip.framework.drc.console.enums;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.platform.dal.dao.DalPojo;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-12
 */
public enum TableEnum {

    DC_TABLE("dc_tbl", "dc_name",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            DcTblDao dao = DalUtils.getInstance().getDcTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `dc_tbl` WHERE `dc_name`=?";
        }
    },

    RESOURCE_TABLE("resource_tbl", "ip",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ResourceTblDao dao = DalUtils.getInstance().getResourceTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `resource_tbl` WHERE `ip`=?";
        }
    },

    PROXY_TABLE("proxy_tbl", "uri",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ProxyTblDao dao = DalUtils.getInstance().getProxyTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `proxy_tbl` WHERE `uri`=?";
        }
    },

    BU_TABLE("bu_tbl", "bu_name",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            BuTblDao dao = DalUtils.getInstance().getBuTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `bu_tbl` WHERE `bu_name`=?";
        }
    },

    CLUSTER_TABLE("cluster_tbl", "cluster_name",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ClusterTblDao dao = DalUtils.getInstance().getClusterTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `cluster_tbl` WHERE `cluster_name`=?";
        }
    },

    MHA_TABLE("mha_tbl", "mha_name",Types.VARCHAR) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            MhaTblDao dao = DalUtils.getInstance().getMhaTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `mha_tbl` WHERE `mha_name`=?";
        }
    },

    MHA_GROUP_TABLE("mha_group_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            MhaGroupTblDao dao = DalUtils.getInstance().getMhaGroupTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for mha_group_tbl");
        }
    },

    ZOOKEEPER_TABLE("zookeeper_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ZookeeperTblDao dao = DalUtils.getInstance().getZookeeperTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for zookeeper_tbl");
        }
    },

    CLUSTER_MANAGER_TABLE("cluster_manager_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ClusterManagerTblDao dao = DalUtils.getInstance().getClusterManagerTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for cluster_manager_tbl");
        }
    },

    CLUSTER_MHA_MAP_TABLE("cluster_mha_map_tbl", "cluster_id,mha_id",Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ClusterMhaMapTblDao dao = DalUtils.getInstance().getClusterMhaMapTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for cluster_mha_map_tbl");
        }
    },

    GROUP_MAPPING_TABLE("group_mapping_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            GroupMappingTblDao dao = DalUtils.getInstance().getGroupMappingTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for group_mapping_tbl");
        }
    },

    MACHINE_TABLE("machine_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            MachineTblDao dao = DalUtils.getInstance().getMachineTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for machine_tbl");
        }
    },

    REPLICATOR_TABLE("replicator_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ReplicatorTblDao dao = DalUtils.getInstance().getReplicatorTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for replicator_tbl");
        }
    },

    APPLIER_GROUP_TABLE("applier_group_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ApplierGroupTblDao dao = DalUtils.getInstance().getApplierGroupTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for applier_group_tbl");
        }
    },

    APPLIER_TABLE("applier_tbl", null,Types.NULL) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ApplierTblDao dao = DalUtils.getInstance().getApplierTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            throw new UnsupportedOperationException("not support for applier_tbl");
        }
    },

    REPLICATOR_GROUP_TABLE("replicator_group_tbl", "mha_id",Types.BIGINT) {
        @Override
        public List<DalPojo> getAllPojos() throws SQLException {
            ReplicatorGroupTblDao dao = DalUtils.getInstance().getReplicatorGroupTblDao();
            return dao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        }

        @Override
        public String selectById() {
            return "SELECT `id` FROM `replicator_group_tbl` WHERE `mha_id`=?";
        }
    };

    private String name;

    private String uniqueKey;
    
    private int uniqueKeyType;

    TableEnum(String name, String uniqueKey,int uniqueKeyType) {
        this.name = name;
        this.uniqueKey = uniqueKey;
        this.uniqueKeyType = uniqueKeyType;
    }

    public String getName() {
        return name;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }
    
    public int getUniqueKeyType() {
        return uniqueKeyType;
    }

    public abstract List<DalPojo> getAllPojos() throws SQLException;

    public abstract String selectById();
}
