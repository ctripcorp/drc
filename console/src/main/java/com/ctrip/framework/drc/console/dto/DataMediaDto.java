package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName DataMediaDto
 * @Author haodongPan
 * @Date 2023/1/3 15:38
 * @Version: $
 */
public class DataMediaDto {

    private long id;
    
    private long applierGroupId;

    private String namespace;

    private String name;

    // see DataMediaTypeEnum,default 0
    private int type;

    private long dataMediaSourceId;

    public DataMediaTbl transferTo() {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        if (id != 0) {
            dataMediaTbl.setId(id);
        }
        if (applierGroupId == 0) {
            throw new IllegalArgumentException("applierGroupId shouldn't be zero");
        } else {
            dataMediaTbl.setApplierGroupId(applierGroupId);
        }
        if (StringUtils.isBlank(namespace)) {
            throw new IllegalArgumentException("namespace shouldn't be blank");
        } else {
            dataMediaTbl.setNamespcae(namespace);
        }
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("name shouldn't be blank");
        } else {
            dataMediaTbl.setName(name);
        }
        dataMediaTbl.setType(type);
        if (dataMediaSourceId == 0) {
            throw new IllegalArgumentException("dataMediaSourceId shouldn't be zero");
        } else {
            dataMediaTbl.setDataMediaSourceId(dataMediaSourceId);
        }
        return dataMediaTbl;
    }

    @Override
    public String toString() {
        return "DataMediaDto{" +
                "id=" + id +
                ", applierGroupId=" + applierGroupId +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getDataMediaSourceId() {
        return dataMediaSourceId;
    }

    public void setDataMediaSourceId(long dataMediaSourceId) {
        this.dataMediaSourceId = dataMediaSourceId;
    }
}
