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

    private String Namespace;

    private String name;

    private Integer type;

    private Long dataMediaSourceId;

    public DataMediaTbl toDataMediaTbl() throws IllegalArgumentException {

        if(!prerequisite()) {
            throw new IllegalArgumentException("some args should be not null: " + this);
        }
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setNamespcae(Namespace);
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
                && StringUtils.isNotBlank(this.Namespace)
                && type != null
                && dataMediaSourceId != null;
    }

    @Override
    public String toString() {
        return "DataMediaDto{" +
                "id=" + id +
                ", Namespace='" + Namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNamespace() {
        return Namespace;
    }

    public void setNamespace(String Namespace) {
        this.Namespace = Namespace;
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
}
