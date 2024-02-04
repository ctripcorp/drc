package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2024/1/31 14:32
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "application_form_tbl")
public class ApplicationFormTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * bu
     */
    @Column(name = "bu_name")
    @Type(value = Types.VARCHAR)
    private String buName;

    /**
     * 库名
     */
    @Column(name = "db_name")
    @Type(value = Types.VARCHAR)
    private String dbName;

    /**
     * 表名
     */
    @Column(name = "table_name")
    @Type(value = Types.VARCHAR)
    private String tableName;

    /**
     * 源region
     */
    @Column(name = "src_region")
    @Type(value = Types.VARCHAR)
    private String srcRegion;

    /**
     * 目标region
     */
    @Column(name = "dst_region")
    @Type(value = Types.VARCHAR)
    private String dstRegion;

    /**
     * 同步方式 0-单向 1-双向
     */
    @Column(name = "replication_type")
    @Type(value = Types.TINYINT)
    private Integer replicationType;

    /**
     * tps
     */
    @Column(name = "tps")
    @Type(value = Types.VARCHAR)
    private String tps;

    /**
     * 业务描述
     */
    @Column(name = "description")
    @Type(value = Types.VARCHAR)
    private String description;

    /**
     * 中断影响
     */
    @Column(name = "disruption_impact")
    @Type(value = Types.VARCHAR)
    private String disruptionImpact;

    /**
     * 过滤方式:ALL, UDL
     */
    @Column(name = "filter_type")
    @Type(value = Types.VARCHAR)
    private String filterType;

    /**
     * tag
     */
    @Column(name = "tag")
    @Type(value = Types.VARCHAR)
    private String tag;

    /**
     * 同步存量 0-否 1-是
     */
    @Column(name = "flush_existing_data")
    @Type(value = Types.TINYINT)
    private Integer flushExistingData;

    /**
     * 订单相关 0-否 1-否
     */
    @Column(name = "order_related")
    @Type(value = Types.TINYINT)
    private Integer orderRelated;

    /**
     * 同步开始位点
     */
    @Column(name = "gtid_init")
    @Type(value = Types.VARCHAR)
    private String gtidInit;

    /**
     * 备注
     */
    @Column(name = "remark")
    @Type(value = Types.VARCHAR)
    private String remark;

    /**
     * 扩展字段
     */
    @Column(name = "extend")
    @Type(value = Types.VARCHAR)
    private String extend;

    /**
     * 是否删除, 0-否; 1-是
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * 创建时间
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
    }

    public String getTps() {
        return tps;
    }

    public void setTps(String tps) {
        this.tps = tps;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDisruptionImpact() {
        return disruptionImpact;
    }

    public void setDisruptionImpact(String disruptionImpact) {
        this.disruptionImpact = disruptionImpact;
    }

    public String getFilterType() {
        return filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }

    public Integer getFlushExistingData() {
        return flushExistingData;
    }

    public void setFlushExistingData(Integer flushExistingData) {
        this.flushExistingData = flushExistingData;
    }

    public Integer getOrderRelated() {
        return orderRelated;
    }

    public void setOrderRelated(Integer orderRelated) {
        this.orderRelated = orderRelated;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
