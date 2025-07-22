package com.ctrip.framework.drc.console.dto.v2;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by shiruixin
 * 2025/7/22 15:17
 */
public class MachineDtoTest {

    @Test
    public void testSetMethod() {
        MachineDto machineDto = new MachineDto();

        machineDto.setIp("ip");
        machineDto.setPort(55944);
        machineDto.setMaster(false);
        Assert.assertFalse(machineDto.getMaster());
        Assert.assertEquals("ip", machineDto.getIp());
        Assert.assertTrue(machineDto.getPort().intValue() == 55944);

    }
}