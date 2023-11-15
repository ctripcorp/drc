package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/10/30 19:51
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_auto_handle_batch_tbl")
public class ConflictAutoHandleBatchTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 源mha名称
     */
    @Column(name = "src_mha_name")
    @Type(value = Types.VARCHAR)
    private String srcMhaName;

    /**
     * 目标mha名称
     */
    @Column(name = "dst_mha_name")
    @Type(value = Types.VARCHAR)
    private String dstMhaName;

    /**
     * 写入端mha 0-目标端mha, 1-源mha
     */
    @Column(name = "target_mha_type")
    @Type(value = Types.TINYINT)
    private Integer targetMhaType;

    /**
     * 库名
     */
    @Column(name = "db_name")
    @Type(value = Types.VARCHAR)
    private String dbName;

    /**
     * 表名,list json
     */
    @Column(name = "table_name")
    @Type(value = Types.VARCHAR)
    private String tableName;

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


    /**
     * 是否执行过, 0-否; 1-是
     */
    @Column(name = "status")
    @Type(value = Types.TINYINT)
    private Integer status;

    /**
     * 备注
     */
    @Column(name = "remark")
    @Type(value = Types.VARCHAR)
    private String remark;

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public Integer getTargetMhaType() {
        return targetMhaType;
    }

    public void setTargetMhaType(Integer targetMhaType) {
        this.targetMhaType = targetMhaType;
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
