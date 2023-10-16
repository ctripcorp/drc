package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/9/26 10:52
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_trx_log_tbl")
public class ConflictTrxLogTbl {
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
     * 事务id
     */
    @Column(name = "gtid")
    @Type(value = Types.VARCHAR)
    private String gtid;

    /**
     * 事务影响行数
     */
    @Column(name = "trx_rows_num")
    @Type(value = Types.BIGINT)
    private Long trxRowsNum;

    /**
     * 冲突影响行数
     */
    @Column(name = "cfl_rows_num")
    @Type(value = Types.BIGINT)
    private Long cflRowsNum;

    /**
     * 事务处理结果: 0-commit 1-rollback
     */
    @Column(name = "trx_result")
    @Type(value = Types.INTEGER)
    private Integer trxResult;

    /**
     * 事务处理时间
     */
    @Column(name = "handle_time")
    @Type(value = Types.BIGINT)
    private Long handleTime;

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

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public Long getTrxRowsNum() {
        return trxRowsNum;
    }

    public void setTrxRowsNum(Long trxRowsNum) {
        this.trxRowsNum = trxRowsNum;
    }

    public Long getCflRowsNum() {
        return cflRowsNum;
    }

    public void setCflRowsNum(Long cflRowsNum) {
        this.cflRowsNum = cflRowsNum;
    }

    public Integer getTrxResult() {
        return trxResult;
    }

    public void setTrxResult(Integer trxResult) {
        this.trxResult = trxResult;
    }

    public Long getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(Long handleTime) {
        this.handleTime = handleTime;
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
