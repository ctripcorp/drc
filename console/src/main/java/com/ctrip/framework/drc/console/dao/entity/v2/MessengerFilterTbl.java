package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/5/31 17:23
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "messenger_filter_tbl")
public class MessengerFilterTbl {

    /**
     * primary key
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * properties
     */
    @Column(name = "gtid_init")
    @Type(value = Types.LONGVARCHAR)
    private String properties;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }
}
