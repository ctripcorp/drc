package com.ctrip.framework.drc.core.service.enums;

import com.ctrip.framework.drc.core.service.exceptions.UnexpectedEnumValueException;
import com.ctrip.framework.foundation.Env;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-11
 */
public enum EnvEnum {

    LOCAL("local",
            Env.LOCAL),
    FAT("fat",
            Env.FAT),

    LPT("lpt",
            Env.LPT),
    UAT("uat",
            Env.UAT),
    PRODUCT("product",
            Env.PRO);

    private String env;

    private Env foundationEnv;


    EnvEnum(String env,  Env foundationEnv) {
        this.env = env;
        this.foundationEnv = foundationEnv;
    }


    public String getEnv(){
        return env;
    }
    

    public Env getFoundationEnv() {
        return foundationEnv;
    }

    public static EnvEnum getEnvEnum(String env) throws UnexpectedEnumValueException {
        for(EnvEnum envEnum: EnvEnum.values()){
            if(envEnum.getEnv().equals(env)){
                return envEnum;
            }
        }
        throw new UnexpectedEnumValueException("wrong env: " + env);
    }

    public static EnvEnum getEnvEnum(Env foundationEnv) throws UnexpectedEnumValueException {
        for(EnvEnum envEnum: EnvEnum.values()){
            if(envEnum.getFoundationEnv().equals(foundationEnv)){
                return envEnum;
            }
        }
        throw new UnexpectedEnumValueException("wrong env: " + foundationEnv.getDescription());
    }

}
