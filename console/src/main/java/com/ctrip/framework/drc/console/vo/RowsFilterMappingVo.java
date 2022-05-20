package com.ctrip.framework.drc.console.vo;


/**
 * @ClassName RowsFilterMappingVo
 * @Author haodongPan
 * @Date 2022/5/18 21:46
 * @Version: $
 */
public class RowsFilterMappingVo {
    
    private Long id ;
    
    
    private Long dataMediaId;

    private String namespace;

    private String name;

    private Integer type;
    
    private Long dataMediaSourceId;

    private String dataMediaSourceName;

    
    private Long rowsFilterId;

    private String rowsFilterName;
    

    public void setDataMediaVo(DataMediaVo dataMediaVo) {
        this.dataMediaId = dataMediaVo.getId();
        this.namespace = dataMediaVo.getNamespace();
        this.name = dataMediaVo.getName();
        this.type = dataMediaVo.getType();
        this.dataMediaSourceId = dataMediaVo.getDataMediaSourceId();
        this.dataMediaSourceName = dataMediaVo.getDataMediaSourceName();
    }

    @Override
    public String toString() {
        return "RowsFilterMappingVo{" +
                "id=" + id +
                ", dataMediaId=" + dataMediaId +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", rowsFilterName='" + rowsFilterName + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDataMediaId() {
        return dataMediaId;
    }

    public void setDataMediaId(Long dataMediaId) {
        this.dataMediaId = dataMediaId;
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

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
    }

    public String getRowsFilterName() {
        return rowsFilterName;
    }

    public void setRowsFilterName(String rowsFilterName) {
        this.rowsFilterName = rowsFilterName;
    }
}
