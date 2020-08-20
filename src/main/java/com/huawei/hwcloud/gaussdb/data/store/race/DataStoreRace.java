package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

public interface DataStoreRace {

    boolean init(String dir);

    void deInit();

    void writeDeltaPacket(DeltaPacket deltaPacket);

    Data readDataByVersion(long key, long version);
}
