package com.ctrip.framework.drc.console.service.v2.external.dba.response;

/**
 * @ClassName SQLDigestInfo
 * @Author haodongPan
 * @Date 2024/2/22 11:34
 * @Version: $
 */
public class SQLDigestInfo {
    private boolean success;
    private Content content;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }

    public static class Content {
        private Digest select;
        private Digest write;

        public Digest getSelect() {
            return select;
        }

        public void setSelect(Digest select) {
            this.select = select;
        }

        public Digest getWrite() {
            return write;
        }

        public void setWrite(Digest write) {
            this.write = write;
        }
    }

    public static class Digest {
        private String digest;
        private String digest_sql;
        private String datachange_lasttime;

        public String getDigest() {
            return digest;
        }

        public void setDigest(String digest) {
            this.digest = digest;
        }

        public String getDigest_sql() {
            return digest_sql;
        }

        public void setDigest_sql(String digest_sql) {
            this.digest_sql = digest_sql;
        }

        public String getDatachange_lasttime() {
            return datachange_lasttime;
        }

        public void setDatachange_lasttime(String datachange_lasttime) {
            this.datachange_lasttime = datachange_lasttime;
        }
    }

}
