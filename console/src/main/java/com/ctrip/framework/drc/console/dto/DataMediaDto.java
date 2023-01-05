package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;

/**
 * @ClassName DataMediaDto
 * @Author haodongPan
 * @Date 2023/1/3 15:38
 * @Version: $
 */
public class DataMediaDto {

    private Long id;
    
    private Long applierGroupId;

    private String namespace;

    private String name;

    // see DataMediaTypeEnum
    private Integer type;

    private Long dataMediaSourceId;

    // todo ,does it necessary?
    private String dataMediaSourceName;

    public DataMediaTbl transferTo() {
        return null;
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
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(Long applierGroupId) {
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
