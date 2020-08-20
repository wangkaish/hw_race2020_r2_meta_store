package com.huawei.hwcloud.gaussdb.data.store.race.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@lombok.Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Data implements Serializable {

    private long key;

    private long version;

    private long[] field;

}
