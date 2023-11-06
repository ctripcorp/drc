package com.ctrip.framework.drc.console.param.api;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/11/6 15:50
 */
public class ApprovalOpenApiResponse {
    private String message;
    private String status;
    private List<ResponseData> data;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }


    public List<ResponseData> getData() {
        return data;
    }

    public void setData(List<ResponseData> data) {
        this.data = data;
    }

    static class ResponseData {
        private String Ticket_ID;

        public ResponseData() {
        }

        public String getTicket_ID() {
            return Ticket_ID;
        }

        public void setTicket_ID(String ticket_ID) {
            Ticket_ID = ticket_ID;
        }
    }
}
