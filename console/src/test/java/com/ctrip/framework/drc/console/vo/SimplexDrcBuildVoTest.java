package com.ctrip.framework.drc.console.vo;

import org.junit.Test;

import static org.junit.Assert.*;

public class SimplexDrcBuildVoTest {

    @Test
    public void testTestToString() {
        SimplexDrcBuildVo simplexDrcBuildVo = new SimplexDrcBuildVo(
                "srcMha",
                "destMha",
                "srcDc",
                "destDc",
                0L,
                0L);
        System.out.println(simplexDrcBuildVo.toString());

    }
}