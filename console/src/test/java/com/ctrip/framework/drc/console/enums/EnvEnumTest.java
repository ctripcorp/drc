package com.ctrip.framework.drc.console.enums;

import com.ctrip.framework.drc.core.service.exceptions.UnexpectedEnumValueException;
import com.ctrip.framework.drc.core.service.enums.EnvEnum;
import com.ctrip.framework.foundation.Env;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/11/12
 */
public class EnvEnumTest {

    @Test(expected = UnexpectedEnumValueException.class)
    public void getEnvException() {
        EnvEnum.getEnvEnum("Exception");
    }

    @Test
    public void getEnv() {
        EnvEnum fat = EnvEnum.getEnvEnum(Env.FAT);
        Assert.assertEquals(fat, EnvEnum.FAT);

        fat = EnvEnum.getEnvEnum(EnvEnum.FAT.getEnv());
        Assert.assertEquals(fat, EnvEnum.FAT);

        EnvEnum local = EnvEnum.getEnvEnum(Env.LOCAL);
        Assert.assertEquals(local, EnvEnum.LOCAL);

        local = EnvEnum.getEnvEnum(EnvEnum.LOCAL.getEnv());
        Assert.assertEquals(local, EnvEnum.LOCAL);
    }
}