package com.ctrip.framework.drc.console.service.v2.external.dba.response;

import java.util.List;

/**
 * @ClassName MemberList
 * @Author haodongPan
 * @Date 2023/8/25 10:58
 * @Version: $
 */
public class Data {
    private List<MemberInfo> memberlist;
    public void setMemberlist(List<MemberInfo> memberlist) {
        this.memberlist = memberlist;
    }
    public List<MemberInfo> getMemberlist() {
        return memberlist;
    }
}
