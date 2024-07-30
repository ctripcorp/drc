package com.ctrip.framework.drc.service.soa;

import com.ctriposs.baiji.exception.BaijiRuntimeException;
import com.ctriposs.baiji.rpc.common.*;
import com.ctriposs.baiji.rpc.common.types.ResponseStatusType;
import com.ctriposs.baiji.schema.*;
import com.ctriposs.baiji.specific.*;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.io.Serializable;

@SuppressWarnings("all")
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonPropertyOrder({
        "filterPass",
        "resultMsg",
        "resultCode",
        "responseStatus"
})
public class FilterRowResponseType implements SpecificRecord, HasResponseStatus, Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    public static final transient Schema SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"FilterRowResponseType\",\"namespace\":\"" + FilterRowResponseType.class.getPackage().getName() + "\",\"doc\":null,\"fields\":[{\"name\":\"filterPass\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"resultMsg\",\"type\":[\"string\",\"null\"]},{\"name\":\"resultCode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ResponseStatus\",\"type\":[{\"type\":\"record\",\"name\":\"ResponseStatusType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"fields\":[{\"name\":\"Timestamp\",\"type\":[\"datetime\",\"null\"]},{\"name\":\"Ack\",\"type\":[{\"type\":\"enum\",\"name\":\"AckCodeType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"symbols\":[\"Success\",\"Failure\",\"Warning\",\"PartialFailure\"]},\"null\"]},{\"name\":\"Errors\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ErrorDataType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"fields\":[{\"name\":\"Message\",\"type\":[\"string\",\"null\"]},{\"name\":\"ErrorCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"StackTrace\",\"type\":[\"string\",\"null\"]},{\"name\":\"SeverityCode\",\"type\":[{\"type\":\"enum\",\"name\":\"SeverityCodeType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"symbols\":[\"Error\",\"Warning\"]},\"null\"]},{\"name\":\"ErrorFields\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ErrorFieldType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"fields\":[{\"name\":\"FieldName\",\"type\":[\"string\",\"null\"]},{\"name\":\"ErrorCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"Message\",\"type\":[\"string\",\"null\"]}]}},\"null\"]},{\"name\":\"ErrorClassification\",\"type\":[{\"type\":\"enum\",\"name\":\"ErrorClassificationCodeType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"symbols\":[\"ServiceError\",\"ValidationError\",\"FrameworkError\",\"SLAError\",\"SecurityError\"]},\"null\"]}]}},\"null\"]},{\"name\":\"Build\",\"type\":[\"string\",\"null\"]},{\"name\":\"Version\",\"type\":[\"string\",\"null\"]},{\"name\":\"Extension\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ExtensionType\",\"namespace\":\"com.ctriposs.baiji.rpc.common.types\",\"doc\":null,\"fields\":[{\"name\":\"Id\",\"type\":[\"string\",\"null\"]},{\"name\":\"Version\",\"type\":[\"string\",\"null\"]},{\"name\":\"ContentType\",\"type\":[\"string\",\"null\"]},{\"name\":\"Value\",\"type\":[\"string\",\"null\"]}]}},\"null\"]}]},\"null\"]}]}");

    @Override
    @JsonIgnore
    public Schema getSchema() { return SCHEMA; }

    public FilterRowResponseType(
            Boolean filterPass,
            String resultMsg,
            Integer resultCode,
            ResponseStatusType responseStatus) {
        this.filterPass = filterPass;
        this.resultMsg = resultMsg;
        this.resultCode = resultCode;
        this.responseStatus = responseStatus;
    }

    public FilterRowResponseType() {
    }

    /**
     * true 则通过行过滤，可同步
     */
    @JsonProperty("filterPass")
    private Boolean filterPass;

    /**
     * 错误信息
     */
    @JsonProperty("resultMsg")
    private String resultMsg;

    /**
     * 0成功，其他失败
     */
    @JsonProperty("resultCode")
    private Integer resultCode;

    @JsonProperty("ResponseStatus")
    private ResponseStatusType responseStatus;

    /**
     * true 则通过行过滤，可同步
     */
    public Boolean isFilterPass() {
        return filterPass;
    }

    /**
     * true 则通过行过滤，可同步
     */
    public void setFilterPass(final Boolean filterPass) {
        this.filterPass = filterPass;
    }

    /**
     * 错误信息
     */
    public String getResultMsg() {
        return resultMsg;
    }

    /**
     * 错误信息
     */
    public void setResultMsg(final String resultMsg) {
        this.resultMsg = resultMsg;
    }

    /**
     * 0成功，其他失败
     */
    public Integer getResultCode() {
        return resultCode;
    }

    /**
     * 0成功，其他失败
     */
    public void setResultCode(final Integer resultCode) {
        this.resultCode = resultCode;
    }
    public ResponseStatusType getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(final ResponseStatusType responseStatus) {
        this.responseStatus = responseStatus;
    }

    // Used by DatumWriter. Applications should not call.
    public Object get(int fieldPos) {
        switch (fieldPos) {
            case 0: return this.filterPass;
            case 1: return this.resultMsg;
            case 2: return this.resultCode;
            case 3: return this.responseStatus;
            default: throw new BaijiRuntimeException("Bad index " + fieldPos + " in get()");
        }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int fieldPos, Object fieldValue) {
        switch (fieldPos) {
            case 0: this.filterPass = (Boolean)fieldValue; break;
            case 1: this.resultMsg = (String)fieldValue; break;
            case 2: this.resultCode = (Integer)fieldValue; break;
            case 3: this.responseStatus = (ResponseStatusType)fieldValue; break;
            default: throw new BaijiRuntimeException("Bad index " + fieldPos + " in put()");
        }
    }

    @Override
    public Object get(String fieldName) {
        Schema schema = getSchema();
        if (!(schema instanceof RecordSchema)) {
            return null;
        }
        Field field = ((RecordSchema) schema).getField(fieldName);
        return field != null ? get(field.getPos()) : null;
    }

    @Override
    public void put(String fieldName, Object fieldValue) {
        Schema schema = getSchema();
        if (!(schema instanceof RecordSchema)) {
            return;
        }
        Field field = ((RecordSchema) schema).getField(fieldName);
        if (field != null) {
            put(field.getPos(), fieldValue);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        final FilterRowResponseType other = (FilterRowResponseType)obj;
        return
                Objects.equal(this.filterPass, other.filterPass) &&
                        Objects.equal(this.resultMsg, other.resultMsg) &&
                        Objects.equal(this.resultCode, other.resultCode) &&
                        Objects.equal(this.responseStatus, other.responseStatus);
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = 31 * result + (this.filterPass == null ? 0 : this.filterPass.hashCode());
        result = 31 * result + (this.resultMsg == null ? 0 : this.resultMsg.hashCode());
        result = 31 * result + (this.resultCode == null ? 0 : this.resultCode.hashCode());
        result = 31 * result + (this.responseStatus == null ? 0 : this.responseStatus.hashCode());

        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("filterPass", filterPass)
                .add("resultMsg", resultMsg)
                .add("resultCode", resultCode)
                .add("responseStatus", responseStatus)
                .toString();
    }
}

