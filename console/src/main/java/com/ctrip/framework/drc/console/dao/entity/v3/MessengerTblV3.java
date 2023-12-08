package com.ctrip.framework.drc.console.dao.entity.v3;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author zyn郑永念
 * @date 2023-11-14
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "messenger_tbl_v3")
public class MessengerTblV3 implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * messenger group id
     */
    @Column(name = "messenger_group_id")
    @Type(value = Types.BIGINT)
    private Long messengerGroupId;

    /**
     * DRC资源机器id
     */
    @Column(name = "resource_id")
    @Type(value = Types.BIGINT)
    private Long resourceId;

    /**
     * 端口
     */
    @Column(name = "port")
    @Type(value = Types.INTEGER)
    private Integer port;

    /**
     * 是否删除, 0:否; 1:是
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

    public Long getMessengerGroupId() {
        return messengerGroupId;
    }

    public void setMessengerGroupId(Long messengerGroupId) {
        this.messengerGroupId = messengerGroupId;
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
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