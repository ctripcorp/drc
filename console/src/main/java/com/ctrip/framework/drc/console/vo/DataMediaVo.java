package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;

/**
 * @ClassName DataMediaVo
 * @Author haodongPan
 * @Date 2022/5/17 15:01
 * @Version: $
 */
public class DataMediaVo {
    private Long id;

    private String namespace;

    private String name;

    private Integer type;

    private Long dataMediaSourceId;
    
    private String dataMediaSourceName;


    @Override
    public String toString() {
        return "DataMediaVo{" +
                "id=" + id +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                '}';
    }

    public DataMediaVo(DataMediaTbl dataMediaTbl) {
        this.id  = dataMediaTbl.getId();
        this.namespace = dataMediaTbl.getNamespcae();
        this.name = dataMediaTbl.getName();
        this.type = dataMediaTbl.getType();
        this.dataMediaSourceId = dataMediaTbl.getDataMediaSourceId();
    }
    
    public DataMediaVo() {}
    
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
