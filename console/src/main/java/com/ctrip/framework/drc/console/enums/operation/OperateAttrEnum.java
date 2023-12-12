package com.ctrip.framework.drc.console.enums.operation;

/**
 * @ClassName OperateAttrEnum
 * @Author haodongPan
 * @Date 2023/12/11 15:02
 * @Version: $
 */
public enum OperateAttrEnum {
    ADD("新增","add"),
    UPDATE("更新","update"),
    DELETE("删除","delete"),
    QUERY("查询","query");
    
    private String name;
    private String val;
    
    OperateAttrEnum(String name, String val) {
        this.name = name;
        this.val = val;
    }
    
    public String getName() {
        return name;
    }
    
    public String getVal() {
        return val;
    }
    

}
