package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import org.apache.commons.lang3.StringUtils;



/**
 * @ClassName DataMediaDto
 * @Author haodongPan
 * @Date 2022/5/6 20:59
 * @Version: $
 */
public class DataMediaDto {

    private Long id;

    private String namespace;

    private String name;

    private Integer type;

    private Long dataMediaSourceId;
    
    private String dataMediaSourceName;

    public DataMediaTbl toDataMediaTbl() throws IllegalArgumentException {

        if(!prerequisite()) {
            throw new IllegalArgumentException("some args should be not null: " + this);
        }
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setNamespcae(namespace);
        dataMediaTbl.setName(name);
        dataMediaTbl.setType(type);
        dataMediaTbl.setDataMediaSourceId(dataMediaSourceId);
        if (id != null) {
            dataMediaTbl.setId(id);
        }
        return dataMediaTbl;
    }

    private boolean prerequisite() {
        return StringUtils.isNotBlank(this.name)
                && StringUtils.isNotBlank(this.namespace)
                && type != null
                && dataMediaSourceId != null;
    }

    @Override
    public String toString() {
        return "DataMediaDto{" +
                "id=" + id +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getDataMediaSourceId() {
        return dataMediaSourceId;
    }

    public void setDataMediaSourceId(Long dataMediaSourceId) {
        this.dataMediaSourceId = dataMediaSourceId;
    }

    public String getDataMediaSourceName() {
        return dataMediaSourceName;
    }

    public void setDataMediaSourceName(String dataMediaSourceName) {
        this.dataMediaSourceName = dataMediaSourceName;
    }
}
