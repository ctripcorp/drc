package com.ctrip.framework.drc.console.service.remote.qconfig.request;

import java.util.List;

/**
 * @ClassName BatchUpdateRequestBody
 * @Author haodongPan
 * @Date 2023/2/9 15:11
 * @Version: $
 */
public class BatchUpdateRequestBody {
    
    private List<UpdateRequestBody> updateRequestBodies;

    @Override
    public String toString() {
        return "BatchUpdateRequestBody{" +
                "updateRequestBodies=" + updateRequestBodies +
                '}';
    }

    public List<UpdateRequestBody> getUpdateRequestBodies() {
        return updateRequestBodies;
    }

    public void setUpdateRequestBodies(
            List<UpdateRequestBody> updateRequestBodies) {
        this.updateRequestBodies = updateRequestBodies;
    }
}
