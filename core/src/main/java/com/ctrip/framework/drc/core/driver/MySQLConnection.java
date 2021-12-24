package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/20 上午11:06.
 */
public interface MySQLConnection extends Lifecycle {

    void preDump() throws Exception;

    void dump(DumpCallBack callBack) throws IOException;

    void postDump() throws Exception;;

    interface DumpCallBack {
        void onFailure(ResultCode code);
    }
}
