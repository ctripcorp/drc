package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.xpipe.api.codec.Codec;

import java.beans.PropertyEditorSupport;

/**
 * @Author limingdong
 * @create 2020/5/25
 */
public class ForwardInfoEditor extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        setValue(Codec.DEFAULT.decode(text, ForwardInfo.class));
    }

    @Override
    public String getAsText() {
        return Codec.DEFAULT.encode(getValue());
    }
}
