package com.ctrip.framework.drc.console.vo.log;

/**
 * @ClassName OptionView
 * @Author haodongPan
 * @Date 2023/12/11 11:40
 * @Version: $
 */
public class OptionView {
    private String name;
    private String val;

    public OptionView() {
    }

    public OptionView(String name, String val) {
        this.name = name;
        this.val = val;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }
}
