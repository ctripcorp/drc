package com.ctrip.framework.drc.core.driver.util;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/10 上午10:29.
 */
public class LengthCodedStringReader {

    public static final String CODE_PAGE_1252 = "UTF-8";

    private String             encoding;
    private int                index          = 0;      // 数组下标

    public LengthCodedStringReader(String encoding, int startIndex){
        this.encoding = encoding;
        this.index = startIndex;
    }

    public String readLengthCodedString(byte[] data) throws IOException {
        byte[] lengthBytes = ByteHelper.readBinaryCodedLengthBytes(data, getIndex());
        long length = ByteHelper.readLengthCodedBinary(data, getIndex());
        setIndex(getIndex() + lengthBytes.length);
        if (ByteHelper.NULL_LENGTH == length) {
            return null;
        }

        try {
            return new String(ArrayUtils.subarray(data, getIndex(), (int) (getIndex() + length)),
                    encoding == null ? CODE_PAGE_1252 : encoding);
        } finally {
            setIndex((int) (getIndex() + length));
        }

    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
