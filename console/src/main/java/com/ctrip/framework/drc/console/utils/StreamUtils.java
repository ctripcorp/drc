package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class StreamUtils {
    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }


    public static MultiKey getKey(MhaDbReplicationTbl e) {
        return new MultiKey(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId(), e.getReplicationType());
    }

    public static MultiKey getKey(DbReplicationTbl e) {
        return new MultiKey(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId(), e.getReplicationType());
    }

    public static MultiKey getKey(MhaDbReplicationDto e) {
        return new MultiKey(e.getSrc().getMhaDbMappingId(), e.getDst().getMhaDbMappingId(), e.getReplicationType());
    }
    public static MultiKey getReverseKey(MhaDbReplicationTbl e) {
        return new MultiKey(e.getDstMhaDbMappingId(), e.getSrcMhaDbMappingId(), e.getReplicationType());
    }
}
