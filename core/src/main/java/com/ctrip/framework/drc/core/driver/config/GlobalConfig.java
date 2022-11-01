package com.ctrip.framework.drc.core.driver.config;

/**
 * Created by mingdongli
 * 2019/9/21 下午8:11
 */
public interface GlobalConfig {

    // Events are without checksum though its generator
    int BINLOG_CHECKSUM_ALG_OFF = 0;

    // is checksum-capable New Master (NM). CRC32 of zlib algorithm.
    int BINLOG_CHECKSUM_ALG_CRC32 = 1;

    // the cut line: valid alg range is [1, 0x7f].
    int BINLOG_CHECKSUM_ALG_ENUM_END = 2;

    // special value to tag undetermined yet checksum
    int BINLOG_CHECKSUM_ALG_UNDEF = 255;

    int LOG_EVENT_IGNORABLE_F = 0x80;

    String REPLICATOR_REGISTER_PATH = "/replicator/instances";

    String MONITOR_REPLICATOR_REGISTER_PATH = "/monitor/instances";

    String APPLIER_REGISTER_PATH = "/applier/instances";

    String MANAGER_REGISTER_PATH = "/manager/instances";

    String REPLICATOR_UUIDS_PATH = "/replicator/config/uuids";

    String APPLIER_POSITIONS_PATH = "/applier/config/positions";

    String BU = "BBZ";

    String DC = "SHAOY";

    long APP_ID = 100023498;
}
