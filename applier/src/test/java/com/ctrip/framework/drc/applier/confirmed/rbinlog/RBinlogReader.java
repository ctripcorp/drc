package com.ctrip.framework.drc.applier.confirmed.rbinlog;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * @Author Slight
 * Jun 16, 2020
 */
public class RBinlogReader {

    @Test
    public void ReadFirstEventFromRBinlog() throws FileNotFoundException {
        RandomAccessFile raf = new RandomAccessFile("/Users/phyx/rbinlog.0000000200", "rw");
    }

}
