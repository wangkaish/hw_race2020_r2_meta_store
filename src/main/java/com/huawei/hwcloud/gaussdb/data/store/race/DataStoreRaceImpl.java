package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.LongIntScatterMap;
import com.firenio.Options;
import com.firenio.buffer.ByteBuf;
import com.firenio.common.Unsafe;
import com.firenio.log.LoggerFactory;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket.DeltaItem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.huawei.hwcloud.gaussdb.data.store.race.Util.*;

public class DataStoreRaceImpl implements DataStoreRace {

    // versions: 900_0000
    // keys: 450_0000
    // key_per_version: 6

    static final AtomicLong ADD_LOG_COUNT = new AtomicLong();

    static final boolean                  ADD_LOG        = false;
    static final int                      KEY_VALUE_SIZE = 4;
    static final int                      MAX_KEY_SIZE   = 450_0000;
    static final int                      SEGMENT_SIZE   = 64;
    static final int                      DELTA_LEN      = 64 * 5;
    static final int                      DATA_LEN       = DELTA_LEN * KEY_VALUE_SIZE;
    static final int                      INDEX_LEN      = 8 + 4 * KEY_VALUE_SIZE;
    static final int                      SEG_KEY_SIZE   = ((MAX_KEY_SIZE / SEGMENT_SIZE) + 3000);
    static final int                      SEG_DATA_SIZE  = SEG_KEY_SIZE * DELTA_LEN * KEY_VALUE_SIZE;
    static final int                      SEG_INDEX_SIZE = SEG_KEY_SIZE * INDEX_LEN;
    static final ThreadLocal<ReadHelper>  L_READ_HELPER  = ThreadLocal.withInitial(ReadHelper::new);
    static final ThreadLocal<WriteHelper> L_WRITE_HELPER = ThreadLocal.withInitial(WriteHelper::new);
    static final StandardOpenOption[]     READ_OPS       = new StandardOpenOption[]{StandardOpenOption.READ};
    static final StandardOpenOption[]     WRITE_OPS      = new StandardOpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};
    static final Segment[]                SEGMENTS       = new Segment[SEGMENT_SIZE];

    static {
        Options.setBufAutoExpansion(false);
        LoggerFactory.setEnableSLF4JLogger(false);
    }

    static void writeItem(long key, long[] delta, int version, long address, ByteBuffer buf) throws Exception {
        buf.clear();
        compress_delta(delta, address);
        Segment segment = SEGMENTS[get_seg_index(key)];
        segment.writeDeltaPacket(version, key, buf, segment.key_version_map);
    }

    static MappedByteBuffer mmap(String root, String file_name, long size) throws IOException {
        FileChannel      channel = FileChannel.open(Paths.get(root, file_name), WRITE_OPS);
        MappedByteBuffer map     = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        map.order(ByteOrder.LITTLE_ENDIAN);
        return map;
    }

    static void addLog(String log, Object... params) {
        //        L_READ_HELPER.get().log.append(Log.arrayFormat(log, params));
        //        L_READ_HELPER.get().log.append("\n\n");
        log(log, params);
    }

    static int get_seg_index(long key) {
        return (int) ((BitMixer.mix64(key)) & (SEGMENT_SIZE - 1));
    }

    static int get_version_from_index(int index) {
        return index & 0xffffff;
    }

    static int get_size_from_index(int index) {
        return index >>> 24;
    }

    static int to_index(int size, int version) {
        return (size << 24) | version;
    }

    static int update_index(int size, int index) {
        return (size << 24) | get_version_from_index(index);
    }

    static boolean isAddLog() {
        if (ADD_LOG) {
            if (ADD_LOG_COUNT.get() < 100) {
                return ADD_LOG_COUNT.getAndIncrement() < 100;
            }
        }
        return false;
    }

    @Override
    public boolean init(String dir) {
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }
        ByteBuf direct = ByteBuf.direct(4);
        try {
            log("init dir: {}", dir);
            System.gc();
            print_jvm_args();
            File file = new File(dir);
            if (!file.exists()) {
                file.mkdirs();
            }
            ByteBuffer buf = direct.nioWriteBuffer();
            for (int i = 0; i < SEGMENT_SIZE; i++) {
                SEGMENTS[i] = new Segment(dir, i);
                SEGMENTS[i].init(buf);
            }
            System.gc();
        } catch (Exception e) {
            log_and_throw(e);
        } finally {
            direct.release();
        }
        return true;
    }

    @Override
    public void deInit() {
        print_jvm_args();
        log("deInit...");
        for (Segment segment : SEGMENTS) {
            segment.deInit();
        }
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        debug_write_data(deltaPacket);
        WriteHelper write_helper = L_WRITE_HELPER.get();
        long[]      deltas1      = write_helper.deltas1;
        long[]      deltas2      = write_helper.deltas2;
        long        address      = write_helper.address;
        ByteBuffer  buf          = write_helper.buf;
        try {
            int             version = (int) deltaPacket.getVersion();
            List<DeltaItem> items   = deltaPacket.getDeltaItem();
            Util.assign(items.get(0).getDelta(), deltas1);
            Util.assign(items.get(1).getDelta(), deltas2);
            Util.apply(items.get(2).getDelta(), deltas1);
            Util.apply(items.get(3).getDelta(), deltas2);
            Util.apply(items.get(4).getDelta(), deltas1);
            Util.apply(items.get(5).getDelta(), deltas2);
            writeItem(items.get(0).getKey(), deltas1, version, address, buf);
            writeItem(items.get(1).getKey(), deltas2, version, address, buf);
        } catch (Exception e) {
            log_and_throw(e);
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        debug_read_data(key, version);
        int        seg_index = get_seg_index(key);
        ReadHelper helper    = L_READ_HELPER.get();
        Data       data      = helper.data;
        ByteBuffer buf       = helper.buf;
        long       address   = helper.address;
        clear_data(data, key, version);
        long[] field = data.getField();
        if (ADD_LOG) {
//            helper.log.setLength(0);
        }
        Segment segment = SEGMENTS[seg_index];
        try {
            segment.readDataByVersion(field, key, version, address, buf, segment.key_version_map);
        } catch (Exception e) {
            log_and_throw(e);
        }
        if (field[0] == 0) {
            return null;
        }
        return data;
    }

    static class Segment {

        final int               seg;
        final FileChannel       data_channel_w;
        final FileChannel       data_channel_r;
        final MappedByteBuffer  index_buffer;
        final LongIntScatterMap key_version_map;
        final LongIntScatterMap key_version_map_bak;
        final long              index_buffer_address;
        final Lock              write_lock = new ReentrantLock();

        int  key_size;
        long costTime;

        Segment(String root, int seg) throws Exception {
            Path data_path = Paths.get(root, seg + "_data");
            this.seg = seg;
            this.data_channel_w = FileChannel.open(data_path, WRITE_OPS);
            this.data_channel_r = FileChannel.open(data_path, READ_OPS);
            this.index_buffer = mmap(root, seg + "_data_index", SEG_INDEX_SIZE);
            this.key_version_map = new LongIntScatterMap(SEG_KEY_SIZE);
            this.key_version_map_bak = new LongIntScatterMap(1024);
            this.index_buffer_address = Unsafe.address(index_buffer);
        }

        void init(ByteBuffer buf) throws Exception {
            long address = this.index_buffer_address;
            for (int i = 0; i < SEG_KEY_SIZE; i++) {
                int  index_base = INDEX_LEN * i;
                long key        = Unsafe.getLong(address + index_base);
                int  raw_index  = Unsafe.getInt(address + index_base + 8);
                if (raw_index == 0) {
                    break;
                }
                if (key == 0) {
                    break;
                }
                this.key_version_map.put(key, key_size);
                this.key_size++;
            }
            buf.clear();
            this.data_channel_w.write(buf, SEG_DATA_SIZE);
        }

        void deInit() {
            log("seg: {}, map_size: {}, map_bak_size:{}, cost: {}", seg, key_version_map.size(), key_version_map_bak.size(), costTime / 1000000);
        }

        void writeDeltaPacket(int version, long key, ByteBuffer buf, LongIntScatterMap key_version_map) throws Exception {
            Lock lock = this.write_lock;
            lock.lock();
            try {
                long index_buffer_address = this.index_buffer_address;
                int  key_index            = key_version_map.getOrDefault(key, -1);
                if (key_index == -1) {
                    key_index = this.key_size++;
                    key_version_map.put(key, key_index);
                    int index_base = key_index * INDEX_LEN;
                    int data_base  = key_index * DATA_LEN;
                    this.data_channel_w.write(buf, data_base);
                    Unsafe.putLong(index_buffer_address + index_base, key);
                    Unsafe.putInt(index_buffer_address + index_base + 8, to_index(1, version));
                } else {
                    if (key_index < 0) {
                        writeDeltaPacket(version, key, buf, key_version_map_bak);
                        return;
                    }
                    int index_base = key_index * INDEX_LEN;
                    int data_base  = key_index * DATA_LEN;
                    int raw_index  = Unsafe.getInt(index_buffer_address + index_base + 8);
                    int size       = get_size_from_index(raw_index);
                    if (size == KEY_VALUE_SIZE) {
                        if (key_version_map == key_version_map_bak) {
                            log_and_throw(new Exception("key_size_over_flow"));
                        }
                        key_version_map.put(key, key_index | (1 << 31));
                        writeDeltaPacket(version, key, buf, key_version_map_bak);
                        return;
                    }
                    int w_data_base  = data_base + size * DELTA_LEN;
                    int w_index_base = index_base + 8 + size * 4;
                    this.data_channel_w.write(buf, w_data_base);
                    Unsafe.putInt(index_buffer_address + w_index_base, version);
                    Unsafe.putInt(index_buffer_address + index_base + 8, update_index(size + 1, raw_index));
                }
            } finally {
                lock.unlock();
            }
        }

        void readDataByVersion(long[] field, long key, long version, long buf_address, ByteBuffer buf, LongIntScatterMap key_version_map) throws Exception {
            long index_buffer_address = this.index_buffer_address;
            int  raw_key_index        = key_version_map.getOrDefault(key, -1);
            if (raw_key_index != -1) {
                int key_index    = raw_key_index & (~(1 << 31));
                int index_base   = key_index * INDEX_LEN;
                int data_base    = key_index * DATA_LEN;
                int raw_index    = Unsafe.getInt(index_buffer_address + index_base + 8);
                int size         = get_size_from_index(raw_index);
                int get_version0 = get_version_from_index(raw_index);
                buf.clear();
                long start = System.nanoTime();
                this.data_channel_r.read(buf, data_base);
                long past = System.nanoTime() - start;
                costTime += past;
                if (isAddLog()) {
                    addLog("{}, read_start: size: {}, kv_index: {}", seg, size, key_index);
                }
                switch (size) {
                    case 1:
                        read_key_delta(buf_address, field, get_version0, version, DELTA_LEN * 0);
                        break;
                    case 2:
                        read_key_delta(buf_address, field, get_version0, version, DELTA_LEN * 0);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 1), version, DELTA_LEN * 1);
                        break;
                    case 3:
                        read_key_delta(buf_address, field, get_version0, version, DELTA_LEN * 0);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 1), version, DELTA_LEN * 1);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 2), version, DELTA_LEN * 2);
                        break;
                    case 4:
                        read_key_delta(buf_address, field, get_version0, version, DELTA_LEN * 0);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 1), version, DELTA_LEN * 1);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 2), version, DELTA_LEN * 2);
                        read_key_delta(buf_address, field, Unsafe.getInt(index_buffer_address + index_base + 8 + 4 * 3), version, DELTA_LEN * 3);
                        break;
                    default:
                        log_and_throw(new Exception("read_size_error"));
                }
                if (raw_key_index < 0) {
                    readDataByVersion(field, key, version, buf_address, buf, key_version_map_bak);
                }
            }
        }

        void read_key_delta(long data_buffer_address, long[] field, int get_version, long version, int data_base) throws Exception {
            if (isAddLog()) {
                addLog("{}, read_key_delta: get_version: {}, version: {}", seg, get_version, version);
            }
            if (get_version <= version) {
                long address1 = data_buffer_address + data_base;
                if (isAddLog()) {
                    long[] field_temp = new long[64];
                    apply(field_temp, address1);
                    addLog("{}, read_deltas: {}", seg, Arrays.toString(field_temp));
                    for (int i = 0; i < 64; i++) {
                        field[i] += field_temp[i];
                    }
                } else {
                    apply(field, address1);
                }
            }
        }

        @Override
        public String toString() {
            return "Segment{ " + seg + "}";
        }
    }

    static class WriteHelper {

        long[]     deltas1 = new long[64];
        long[]     deltas2 = new long[64];
        ByteBuffer buf     = ByteBuffer.allocateDirect(DELTA_LEN);
        long       address = Unsafe.address(buf);


    }

    static class ReadHelper {

        final Data       data    = new_data();
        final ByteBuffer buf     = ByteBuffer.allocateDirect(DATA_LEN);
        final long       address = Unsafe.address(buf);
//        StringBuilder log = new StringBuilder();

    }

}
