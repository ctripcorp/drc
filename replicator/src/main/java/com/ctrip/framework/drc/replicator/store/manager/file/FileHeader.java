package com.ctrip.framework.drc.replicator.store.manager.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/16 下午10:17.
 */
public class FileHeader {

    private byte[] magic;

    public FileHeader(byte[] magic) {
        this.magic = magic;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 1. write magic
        out.write(magic);

        // end write
        return out.toByteArray();
    }

}