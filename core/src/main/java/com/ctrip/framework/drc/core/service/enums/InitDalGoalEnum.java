package com.ctrip.framework.drc.core.service.enums;


import com.ctrip.framework.drc.core.service.exceptions.UnexpectedEnumValueException;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-11
 */
public enum InitDalGoalEnum {

    INSTANCE("instance","initdatabasedalclusterinstance"),
    USER("user","initdatabasedalclusteruser");

    private String goal;

    private String dalRegisterSuffix;

    public String getGoal() {
        return goal;
    }

    public String getDalRegisterSuffix(){
        return dalRegisterSuffix;
    }

    InitDalGoalEnum(String goal, String dalRegisterSuffix) {
        this.goal = goal;
        this.dalRegisterSuffix = dalRegisterSuffix;
    }

    public static InitDalGoalEnum getInitDalGoalEnum(String goal) throws UnexpectedEnumValueException {
        for(InitDalGoalEnum initDalGoalEnum:InitDalGoalEnum.values()){
            if(initDalGoalEnum.getGoal().equals(goal)){
                return initDalGoalEnum;
            }
        }
        throw new UnexpectedEnumValueException("wrong goal: " + goal);
    }
}
