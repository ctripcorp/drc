package com.ctrip.framework.drc.fetcher.resource.context;

import java.io.IOException;

/**
 * Created by jixinwang on 2022/10/24
 */
public interface MqPosition {

    void updatePosition(String gtid);

    String getPosition() throws IOException;
}
