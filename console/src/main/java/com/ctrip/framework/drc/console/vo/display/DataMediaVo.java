package com.ctrip.framework.drc.console.vo.display;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;

/**
 * @ClassName DataMediaVo
 * @Author haodongPan
 * @Date 2023/1/17 11:16
 * @Version: $
 */
public class DataMediaVo {
    
    private Long id;
    
    private String namespace;
    
    private String name;
    
    
    public static DataMediaVo toVo(DataMediaTbl dataMediaTbl) {
        DataMediaVo vo = new DataMediaVo();
        vo.setId(dataMediaTbl.getId());
        vo.setNamespace(dataMediaTbl.getNamespcae());
        vo.setName(dataMediaTbl.getName());
        return vo;
    }

    @Override
    public String toString() {
        return "DataMediaVo{" +
                "id=" + id +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
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
}
