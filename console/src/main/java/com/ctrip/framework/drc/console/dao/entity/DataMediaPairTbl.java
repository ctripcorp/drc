package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @ClassName DataMediaPairTbl
 * @Author haodongPan
 * @Date 2022/9/30 14:21
 * @Version: $
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "dataMediaPair_tbl")
public class DataMediaPairTbl implements DalPojo {
    
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 0 db->db，1 db->mq
     */
    @Column(name = "type")
    @Type(value = Types.TINYINT)
    private Integer type;
    
    /**
     * group_id
     * applier_group_id/messenger_group_id
     */
    @Column(name = "group_id")
    @Type(value = Types.BIGINT)
    private Long groupId;

    /**
     * see dataMediaTbl
     */
    @Column(name = "src_data_media_name")
    @Type(value = Types.VARCHAR)
    private String srcDataMediaName;

    
    @Column(name = "dest_data_media_name")
    @Type(value = Types.VARCHAR)
    private String destDataMediaName;
    
    /**
     *  保存 java 文件
     *  兼容Otter EventProcessor
     */
    @Column(name = "processor")
    @Type(value = Types.LONGVARCHAR)
    private String processor;

    
    /**
     * json
     * 保存 相关配置
     * 由type 决定 类型
     */
    @Column(name = "properties")
    @Type(value = Types.LONGVARCHAR)
    private String properties;
    
    
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

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public String getSrcDataMediaName() {
        return srcDataMediaName;
    }

    public void setSrcDataMediaName(String srcDataMediaName) {
        this.srcDataMediaName = srcDataMediaName;
    }

    public String getDestDataMediaName() {
        return destDataMediaName;
    }

    public void setDestDataMediaName(String destDataMediaName) {
        this.destDataMediaName = destDataMediaName;
    }

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
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
