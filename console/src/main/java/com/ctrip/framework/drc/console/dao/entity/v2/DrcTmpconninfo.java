package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Timestamp;
import java.sql.Types;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ClassName DrcTmpconninfo
 * @Author haodongPan
 * @Date 2024/6/13 15:18
 * @Version: $
 */

@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "drc_tmpconninfo")
public class DrcTmpconninfo {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * MySQL master实例ip
     */
    @Column(name = "host")
    @Type(value = Types.VARCHAR)
    private String host;

    /**
     * MySQL实例端口
     */
    @Column(name = "port")
    @Type(value = Types.INTEGER)
    private Integer port;

    /**
     * MySQL实例用户名
     */
    @Column(name = "dbuser")
    @Type(value = Types.VARCHAR)
    private String dbUser;
    

    /**
     * 是否删除, 0:否; 1:是
     */
    @Column(name = "password")
    @Type(value = Types.BLOB)
    private Byte[] password;

    /**
     * 创建时间
     */
    @Column(name = "datachange_createTime")
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeCreateTime;

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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public Byte[] getPassword() {
        return password;
    }

    public void setPassword(Byte[] password) {
        this.password = password;
    }

    public Timestamp getDatachangeCreateTime() {
        return datachangeCreateTime;
    }

    public void setDatachangeCreateTime(Timestamp datachangeCreateTime) {
        this.datachangeCreateTime = datachangeCreateTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
