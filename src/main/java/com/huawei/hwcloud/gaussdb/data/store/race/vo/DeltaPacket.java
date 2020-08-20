package com.huawei.hwcloud.gaussdb.data.store.race.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeltaPacket implements Serializable {

    private static final long serialVersionUID = 4750719966645557615L;

    private long version;

    private short deltaCount;

    private List<DeltaItem> deltaItem;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DeltaItem implements Serializable {

        private static final long serialVersionUID = 4750719966645557615L;

        private long key;

        private int[] delta;
    }
}
