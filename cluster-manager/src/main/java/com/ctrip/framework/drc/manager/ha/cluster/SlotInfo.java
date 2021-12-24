package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.codec.JsonCodable;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class SlotInfo  extends JsonCodable implements Cloneable{

    private String serverId;

    private SLOT_STATE slotState = SLOT_STATE.NORMAL;

    private String toServerId;

    public SlotInfo(){

    }

    public SlotInfo(String serverId){
        this.serverId = serverId;
    }

    public void moveingSlot(String toServerId){

        slotState = SLOT_STATE.MOVING;
        this.toServerId = toServerId;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public SLOT_STATE getSlotState() {
        return slotState;
    }

    public String getToServerId() {
        return toServerId;
    }

    public static SlotInfo decode(byte []bytes){
        return JsonCodable.decode(bytes, SlotInfo.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("serverId:" + serverId);
        if(slotState == SLOT_STATE.MOVING){
            sb.append("->toServerId:" + toServerId);
        }
        return sb.toString();
    }

    @Override
    public SlotInfo clone() throws CloneNotSupportedException {

        SlotInfo clone = (SlotInfo) super.clone();
        return clone;
    }
}
