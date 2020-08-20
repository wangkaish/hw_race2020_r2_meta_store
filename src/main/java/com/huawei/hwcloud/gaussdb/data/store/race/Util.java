package com.huawei.hwcloud.gaussdb.data.store.race;

import com.firenio.buffer.ByteBuf;
import com.firenio.common.Unsafe;
import com.firenio.log.DebugUtil;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Util {

    static final AtomicLong LW         = new AtomicLong();
    static final AtomicLong LR         = new AtomicLong();
    static final long       INT_MASK   = 0XFFFFFFFFL;
    static final long       BYTE_MASK  = 0XFFL;
    static final long       DELTA_BASE = 0xFFFFFFFFFL;

    public static long debug_key;

    public static void clear_data(Data data, long key, long version) {
        data.setKey(key);
        data.setVersion(version);
        clear_field(data.getField());
    }

    static void clear_field(long[] field) {
        field[0] = 0;
        field[1] = 0;
        field[2] = 0;
        field[3] = 0;
        field[4] = 0;
        field[5] = 0;
        field[6] = 0;
        field[7] = 0;
        field[8] = 0;
        field[9] = 0;
        field[10] = 0;
        field[11] = 0;
        field[12] = 0;
        field[13] = 0;
        field[14] = 0;
        field[15] = 0;
        field[16] = 0;
        field[17] = 0;
        field[18] = 0;
        field[19] = 0;
        field[20] = 0;
        field[21] = 0;
        field[22] = 0;
        field[23] = 0;
        field[24] = 0;
        field[25] = 0;
        field[26] = 0;
        field[27] = 0;
        field[28] = 0;
        field[29] = 0;
        field[30] = 0;
        field[31] = 0;
        field[32] = 0;
        field[33] = 0;
        field[34] = 0;
        field[35] = 0;
        field[36] = 0;
        field[37] = 0;
        field[38] = 0;
        field[39] = 0;
        field[40] = 0;
        field[41] = 0;
        field[42] = 0;
        field[43] = 0;
        field[44] = 0;
        field[45] = 0;
        field[46] = 0;
        field[47] = 0;
        field[48] = 0;
        field[49] = 0;
        field[50] = 0;
        field[51] = 0;
        field[52] = 0;
        field[53] = 0;
        field[54] = 0;
        field[55] = 0;
        field[56] = 0;
        field[57] = 0;
        field[58] = 0;
        field[59] = 0;
        field[60] = 0;
        field[61] = 0;
        field[62] = 0;
        field[63] = 0;
    }

    public static void apply(long[] field, long address) {
        long l0  = (Unsafe.getLong(address + 0));
        long l1  = (Unsafe.getLong(address + 8));
        long l2  = (Unsafe.getLong(address + 16));
        long l3  = (Unsafe.getLong(address + 24));
        long l4  = (Unsafe.getLong(address + 32));
        long l5  = (Unsafe.getLong(address + 40));
        long l6  = (Unsafe.getLong(address + 48));
        long l7  = (Unsafe.getLong(address + 56));
        long l8  = (Unsafe.getLong(address + 64));
        long l9  = (Unsafe.getLong(address + 72));
        long l10 = (Unsafe.getLong(address + 80));
        long l11 = (Unsafe.getLong(address + 88));
        long l12 = (Unsafe.getLong(address + 96));
        long l13 = (Unsafe.getLong(address + 104));
        long l14 = (Unsafe.getLong(address + 112));
        long l15 = (Unsafe.getLong(address + 120));
        long l16 = (Unsafe.getLong(address + 128));
        long l17 = (Unsafe.getLong(address + 136));
        long l18 = (Unsafe.getLong(address + 144));
        long l19 = (Unsafe.getLong(address + 152));
        long l20 = (Unsafe.getLong(address + 160));
        long l21 = (Unsafe.getLong(address + 168));
        long l22 = (Unsafe.getLong(address + 176));
        long l23 = (Unsafe.getLong(address + 184));
        long l24 = (Unsafe.getLong(address + 192));
        long l25 = (Unsafe.getLong(address + 200));
        long l26 = (Unsafe.getLong(address + 208));
        long l27 = (Unsafe.getLong(address + 216));
        long l28 = (Unsafe.getLong(address + 224));
        long l29 = (Unsafe.getLong(address + 232));
        long l30 = (Unsafe.getLong(address + 240));
        long l31 = (Unsafe.getLong(address + 248));
        long l32 = (Unsafe.getLong(address + 256));
        long l33 = (Unsafe.getLong(address + 264));
        long l34 = (Unsafe.getLong(address + 272));
        long l35 = (Unsafe.getLong(address + 280));
        long l36 = (Unsafe.getLong(address + 288));
        long l37 = (Unsafe.getLong(address + 296));
        long l38 = (Unsafe.getLong(address + 304));
        long l39 = (Unsafe.getLong(address + 312));

        field[0] += ((l0 >>> 32) | ((((l32 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[1] += ((l0 & INT_MASK) | ((((l32 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[2] += ((l1 >>> 32) | ((((l32 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[3] += ((l1 & INT_MASK) | ((((l32 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[4] += ((l2 >>> 32) | ((((l32 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[5] += ((l2 & INT_MASK) | ((((l32 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[6] += ((l3 >>> 32) | ((((l32 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[7] += ((l3 & INT_MASK) | ((((l32 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[8] += ((l4 >>> 32) | ((((l33 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[9] += ((l4 & INT_MASK) | ((((l33 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[10] += ((l5 >>> 32) | ((((l33 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[11] += ((l5 & INT_MASK) | ((((l33 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[12] += ((l6 >>> 32) | ((((l33 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[13] += ((l6 & INT_MASK) | ((((l33 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[14] += ((l7 >>> 32) | ((((l33 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[15] += ((l7 & INT_MASK) | ((((l33 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[16] += ((l8 >>> 32) | ((((l34 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[17] += ((l8 & INT_MASK) | ((((l34 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[18] += ((l9 >>> 32) | ((((l34 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[19] += ((l9 & INT_MASK) | ((((l34 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[20] += ((l10 >>> 32) | ((((l34 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[21] += ((l10 & INT_MASK) | ((((l34 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[22] += ((l11 >>> 32) | ((((l34 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[23] += ((l11 & INT_MASK) | ((((l34 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[24] += ((l12 >>> 32) | ((((l35 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[25] += ((l12 & INT_MASK) | ((((l35 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[26] += ((l13 >>> 32) | ((((l35 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[27] += ((l13 & INT_MASK) | ((((l35 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[28] += ((l14 >>> 32) | ((((l35 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[29] += ((l14 & INT_MASK) | ((((l35 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[30] += ((l15 >>> 32) | ((((l35 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[31] += ((l15 & INT_MASK) | ((((l35 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[32] += ((l16 >>> 32) | ((((l36 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[33] += ((l16 & INT_MASK) | ((((l36 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[34] += ((l17 >>> 32) | ((((l36 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[35] += ((l17 & INT_MASK) | ((((l36 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[36] += ((l18 >>> 32) | ((((l36 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[37] += ((l18 & INT_MASK) | ((((l36 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[38] += ((l19 >>> 32) | ((((l36 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[39] += ((l19 & INT_MASK) | ((((l36 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[40] += ((l20 >>> 32) | ((((l37 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[41] += ((l20 & INT_MASK) | ((((l37 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[42] += ((l21 >>> 32) | ((((l37 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[43] += ((l21 & INT_MASK) | ((((l37 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[44] += ((l22 >>> 32) | ((((l37 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[45] += ((l22 & INT_MASK) | ((((l37 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[46] += ((l23 >>> 32) | ((((l37 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[47] += ((l23 & INT_MASK) | ((((l37 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[48] += ((l24 >>> 32) | ((((l38 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[49] += ((l24 & INT_MASK) | ((((l38 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[50] += ((l25 >>> 32) | ((((l38 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[51] += ((l25 & INT_MASK) | ((((l38 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[52] += ((l26 >>> 32) | ((((l38 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[53] += ((l26 & INT_MASK) | ((((l38 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[54] += ((l27 >>> 32) | ((((l38 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[55] += ((l27 & INT_MASK) | ((((l38 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[56] += ((l28 >>> 32) | ((((l39 >>> 0) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[57] += ((l28 & INT_MASK) | ((((l39 >>> 8) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[58] += ((l29 >>> 32) | ((((l39 >>> 16) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[59] += ((l29 & INT_MASK) | ((((l39 >>> 24) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[60] += ((l30 >>> 32) | ((((l39 >>> 32) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[61] += ((l30 & INT_MASK) | ((((l39 >>> 40) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[62] += ((l31 >>> 32) | ((((l39 >>> 48) & BYTE_MASK)) << 32)) - DELTA_BASE;
        field[63] += ((l31 & INT_MASK) | ((((l39 >>> 56) & BYTE_MASK)) << 32)) - DELTA_BASE;
    }

    static void assign(int[] delta, long[] field) {
        field[0] = delta[0];
        field[1] = delta[1];
        field[2] = delta[2];
        field[3] = delta[3];
        field[4] = delta[4];
        field[5] = delta[5];
        field[6] = delta[6];
        field[7] = delta[7];
        field[8] = delta[8];
        field[9] = delta[9];
        field[10] = delta[10];
        field[11] = delta[11];
        field[12] = delta[12];
        field[13] = delta[13];
        field[14] = delta[14];
        field[15] = delta[15];
        field[16] = delta[16];
        field[17] = delta[17];
        field[18] = delta[18];
        field[19] = delta[19];
        field[20] = delta[20];
        field[21] = delta[21];
        field[22] = delta[22];
        field[23] = delta[23];
        field[24] = delta[24];
        field[25] = delta[25];
        field[26] = delta[26];
        field[27] = delta[27];
        field[28] = delta[28];
        field[29] = delta[29];
        field[30] = delta[30];
        field[31] = delta[31];
        field[32] = delta[32];
        field[33] = delta[33];
        field[34] = delta[34];
        field[35] = delta[35];
        field[36] = delta[36];
        field[37] = delta[37];
        field[38] = delta[38];
        field[39] = delta[39];
        field[40] = delta[40];
        field[41] = delta[41];
        field[42] = delta[42];
        field[43] = delta[43];
        field[44] = delta[44];
        field[45] = delta[45];
        field[46] = delta[46];
        field[47] = delta[47];
        field[48] = delta[48];
        field[49] = delta[49];
        field[50] = delta[50];
        field[51] = delta[51];
        field[52] = delta[52];
        field[53] = delta[53];
        field[54] = delta[54];
        field[55] = delta[55];
        field[56] = delta[56];
        field[57] = delta[57];
        field[58] = delta[58];
        field[59] = delta[59];
        field[60] = delta[60];
        field[61] = delta[61];
        field[62] = delta[62];
        field[63] = delta[63];
    }

    public static void apply(int[] delta, long[] field) {
        field[0] += delta[0];
        field[1] += delta[1];
        field[2] += delta[2];
        field[3] += delta[3];
        field[4] += delta[4];
        field[5] += delta[5];
        field[6] += delta[6];
        field[7] += delta[7];
        field[8] += delta[8];
        field[9] += delta[9];
        field[10] += delta[10];
        field[11] += delta[11];
        field[12] += delta[12];
        field[13] += delta[13];
        field[14] += delta[14];
        field[15] += delta[15];
        field[16] += delta[16];
        field[17] += delta[17];
        field[18] += delta[18];
        field[19] += delta[19];
        field[20] += delta[20];
        field[21] += delta[21];
        field[22] += delta[22];
        field[23] += delta[23];
        field[24] += delta[24];
        field[25] += delta[25];
        field[26] += delta[26];
        field[27] += delta[27];
        field[28] += delta[28];
        field[29] += delta[29];
        field[30] += delta[30];
        field[31] += delta[31];
        field[32] += delta[32];
        field[33] += delta[33];
        field[34] += delta[34];
        field[35] += delta[35];
        field[36] += delta[36];
        field[37] += delta[37];
        field[38] += delta[38];
        field[39] += delta[39];
        field[40] += delta[40];
        field[41] += delta[41];
        field[42] += delta[42];
        field[43] += delta[43];
        field[44] += delta[44];
        field[45] += delta[45];
        field[46] += delta[46];
        field[47] += delta[47];
        field[48] += delta[48];
        field[49] += delta[49];
        field[50] += delta[50];
        field[51] += delta[51];
        field[52] += delta[52];
        field[53] += delta[53];
        field[54] += delta[54];
        field[55] += delta[55];
        field[56] += delta[56];
        field[57] += delta[57];
        field[58] += delta[58];
        field[59] += delta[59];
        field[60] += delta[60];
        field[61] += delta[61];
        field[62] += delta[62];
        field[63] += delta[63];
    }

    public static void compress_delta(long[] delta, long address) {
        delta[0] += DELTA_BASE;
        delta[1] += DELTA_BASE;
        delta[2] += DELTA_BASE;
        delta[3] += DELTA_BASE;
        delta[4] += DELTA_BASE;
        delta[5] += DELTA_BASE;
        delta[6] += DELTA_BASE;
        delta[7] += DELTA_BASE;
        delta[8] += DELTA_BASE;
        delta[9] += DELTA_BASE;
        delta[10] += DELTA_BASE;
        delta[11] += DELTA_BASE;
        delta[12] += DELTA_BASE;
        delta[13] += DELTA_BASE;
        delta[14] += DELTA_BASE;
        delta[15] += DELTA_BASE;
        delta[16] += DELTA_BASE;
        delta[17] += DELTA_BASE;
        delta[18] += DELTA_BASE;
        delta[19] += DELTA_BASE;
        delta[20] += DELTA_BASE;
        delta[21] += DELTA_BASE;
        delta[22] += DELTA_BASE;
        delta[23] += DELTA_BASE;
        delta[24] += DELTA_BASE;
        delta[25] += DELTA_BASE;
        delta[26] += DELTA_BASE;
        delta[27] += DELTA_BASE;
        delta[28] += DELTA_BASE;
        delta[29] += DELTA_BASE;
        delta[30] += DELTA_BASE;
        delta[31] += DELTA_BASE;
        delta[32] += DELTA_BASE;
        delta[33] += DELTA_BASE;
        delta[34] += DELTA_BASE;
        delta[35] += DELTA_BASE;
        delta[36] += DELTA_BASE;
        delta[37] += DELTA_BASE;
        delta[38] += DELTA_BASE;
        delta[39] += DELTA_BASE;
        delta[40] += DELTA_BASE;
        delta[41] += DELTA_BASE;
        delta[42] += DELTA_BASE;
        delta[43] += DELTA_BASE;
        delta[44] += DELTA_BASE;
        delta[45] += DELTA_BASE;
        delta[46] += DELTA_BASE;
        delta[47] += DELTA_BASE;
        delta[48] += DELTA_BASE;
        delta[49] += DELTA_BASE;
        delta[50] += DELTA_BASE;
        delta[51] += DELTA_BASE;
        delta[52] += DELTA_BASE;
        delta[53] += DELTA_BASE;
        delta[54] += DELTA_BASE;
        delta[55] += DELTA_BASE;
        delta[56] += DELTA_BASE;
        delta[57] += DELTA_BASE;
        delta[58] += DELTA_BASE;
        delta[59] += DELTA_BASE;
        delta[60] += DELTA_BASE;
        delta[61] += DELTA_BASE;
        delta[62] += DELTA_BASE;
        delta[63] += DELTA_BASE;

        long l0  = (delta[0] << 32) | (delta[1] & INT_MASK);
        long l1  = (delta[2] << 32) | (delta[3] & INT_MASK);
        long l2  = (delta[4] << 32) | (delta[5] & INT_MASK);
        long l3  = (delta[6] << 32) | (delta[7] & INT_MASK);
        long l4  = (delta[8] << 32) | (delta[9] & INT_MASK);
        long l5  = (delta[10] << 32) | (delta[11] & INT_MASK);
        long l6  = (delta[12] << 32) | (delta[13] & INT_MASK);
        long l7  = (delta[14] << 32) | (delta[15] & INT_MASK);
        long l8  = (delta[16] << 32) | (delta[17] & INT_MASK);
        long l9  = (delta[18] << 32) | (delta[19] & INT_MASK);
        long l10 = (delta[20] << 32) | (delta[21] & INT_MASK);
        long l11 = (delta[22] << 32) | (delta[23] & INT_MASK);
        long l12 = (delta[24] << 32) | (delta[25] & INT_MASK);
        long l13 = (delta[26] << 32) | (delta[27] & INT_MASK);
        long l14 = (delta[28] << 32) | (delta[29] & INT_MASK);
        long l15 = (delta[30] << 32) | (delta[31] & INT_MASK);
        long l16 = (delta[32] << 32) | (delta[33] & INT_MASK);
        long l17 = (delta[34] << 32) | (delta[35] & INT_MASK);
        long l18 = (delta[36] << 32) | (delta[37] & INT_MASK);
        long l19 = (delta[38] << 32) | (delta[39] & INT_MASK);
        long l20 = (delta[40] << 32) | (delta[41] & INT_MASK);
        long l21 = (delta[42] << 32) | (delta[43] & INT_MASK);
        long l22 = (delta[44] << 32) | (delta[45] & INT_MASK);
        long l23 = (delta[46] << 32) | (delta[47] & INT_MASK);
        long l24 = (delta[48] << 32) | (delta[49] & INT_MASK);
        long l25 = (delta[50] << 32) | (delta[51] & INT_MASK);
        long l26 = (delta[52] << 32) | (delta[53] & INT_MASK);
        long l27 = (delta[54] << 32) | (delta[55] & INT_MASK);
        long l28 = (delta[56] << 32) | (delta[57] & INT_MASK);
        long l29 = (delta[58] << 32) | (delta[59] & INT_MASK);
        long l30 = (delta[60] << 32) | (delta[61] & INT_MASK);
        long l31 = (delta[62] << 32) | (delta[63] & INT_MASK);
        long l32 = ((delta[0] >>> 32) << 0) | ((delta[1] >>> 32) << 8) | ((delta[2] >>> 32) << 16) | ((delta[3] >>> 32) << 24) | ((delta[4] >>> 32) << 32) | ((delta[5] >>> 32) << 40) | ((delta[6] >>> 32) << 48) | ((delta[7] >>> 32) << 56);
        long l33 = ((delta[8] >>> 32) << 0) | ((delta[9] >>> 32) << 8) | ((delta[10] >>> 32) << 16) | ((delta[11] >>> 32) << 24) | ((delta[12] >>> 32) << 32) | ((delta[13] >>> 32) << 40) | ((delta[14] >>> 32) << 48) | ((delta[15] >>> 32) << 56);
        long l34 = ((delta[16] >>> 32) << 0) | ((delta[17] >>> 32) << 8) | ((delta[18] >>> 32) << 16) | ((delta[19] >>> 32) << 24) | ((delta[20] >>> 32) << 32) | ((delta[21] >>> 32) << 40) | ((delta[22] >>> 32) << 48) | ((delta[23] >>> 32) << 56);
        long l35 = ((delta[24] >>> 32) << 0) | ((delta[25] >>> 32) << 8) | ((delta[26] >>> 32) << 16) | ((delta[27] >>> 32) << 24) | ((delta[28] >>> 32) << 32) | ((delta[29] >>> 32) << 40) | ((delta[30] >>> 32) << 48) | ((delta[31] >>> 32) << 56);
        long l36 = ((delta[32] >>> 32) << 0) | ((delta[33] >>> 32) << 8) | ((delta[34] >>> 32) << 16) | ((delta[35] >>> 32) << 24) | ((delta[36] >>> 32) << 32) | ((delta[37] >>> 32) << 40) | ((delta[38] >>> 32) << 48) | ((delta[39] >>> 32) << 56);
        long l37 = ((delta[40] >>> 32) << 0) | ((delta[41] >>> 32) << 8) | ((delta[42] >>> 32) << 16) | ((delta[43] >>> 32) << 24) | ((delta[44] >>> 32) << 32) | ((delta[45] >>> 32) << 40) | ((delta[46] >>> 32) << 48) | ((delta[47] >>> 32) << 56);
        long l38 = ((delta[48] >>> 32) << 0) | ((delta[49] >>> 32) << 8) | ((delta[50] >>> 32) << 16) | ((delta[51] >>> 32) << 24) | ((delta[52] >>> 32) << 32) | ((delta[53] >>> 32) << 40) | ((delta[54] >>> 32) << 48) | ((delta[55] >>> 32) << 56);
        long l39 = ((delta[56] >>> 32) << 0) | ((delta[57] >>> 32) << 8) | ((delta[58] >>> 32) << 16) | ((delta[59] >>> 32) << 24) | ((delta[60] >>> 32) << 32) | ((delta[61] >>> 32) << 40) | ((delta[62] >>> 32) << 48) | ((delta[63] >>> 32) << 56);

        Unsafe.putLong(address + 0, l0);
        Unsafe.putLong(address + 8, l1);
        Unsafe.putLong(address + 16, l2);
        Unsafe.putLong(address + 24, l3);
        Unsafe.putLong(address + 32, l4);
        Unsafe.putLong(address + 40, l5);
        Unsafe.putLong(address + 48, l6);
        Unsafe.putLong(address + 56, l7);
        Unsafe.putLong(address + 64, l8);
        Unsafe.putLong(address + 72, l9);
        Unsafe.putLong(address + 80, l10);
        Unsafe.putLong(address + 88, l11);
        Unsafe.putLong(address + 96, l12);
        Unsafe.putLong(address + 104, l13);
        Unsafe.putLong(address + 112, l14);
        Unsafe.putLong(address + 120, l15);
        Unsafe.putLong(address + 128, l16);
        Unsafe.putLong(address + 136, l17);
        Unsafe.putLong(address + 144, l18);
        Unsafe.putLong(address + 152, l19);
        Unsafe.putLong(address + 160, l20);
        Unsafe.putLong(address + 168, l21);
        Unsafe.putLong(address + 176, l22);
        Unsafe.putLong(address + 184, l23);
        Unsafe.putLong(address + 192, l24);
        Unsafe.putLong(address + 200, l25);
        Unsafe.putLong(address + 208, l26);
        Unsafe.putLong(address + 216, l27);
        Unsafe.putLong(address + 224, l28);
        Unsafe.putLong(address + 232, l29);
        Unsafe.putLong(address + 240, l30);
        Unsafe.putLong(address + 248, l31);
        Unsafe.putLong(address + 256, l32);
        Unsafe.putLong(address + 264, l33);
        Unsafe.putLong(address + 272, l34);
        Unsafe.putLong(address + 280, l35);
        Unsafe.putLong(address + 288, l36);
        Unsafe.putLong(address + 296, l37);
        Unsafe.putLong(address + 304, l38);
        Unsafe.putLong(address + 312, l39);
    }

    static void log(String msg, Object... params) {
        DebugUtil.info(" [LIBMETA_MGR] " + msg, params);
        //        DebugUtil.info(msg, params);
    }

    static Object log_and_throw(Throwable e) {
        log(e.getMessage());
        DebugUtil.error(e.getMessage(), e);
        throw new RuntimeException(e);
    }

    static Data new_data() {
        Data data = new Data();
        data.setField(new long[64]);
        return data;
    }

    static void debug_write_data(DeltaPacket p) {
        long andIncrement = LW.get();
        if (andIncrement == 0) {
            if (LW.getAndIncrement() == 0) {
                DeltaPacket.DeltaItem item = p.getDeltaItem().get(0);
                log("writeData: {}, {} , {} , {}, {}, {}",
                        p.getDeltaItem().get(0).getKey(),
                        p.getDeltaItem().get(1).getKey(),
                        p.getDeltaItem().get(2).getKey(),
                        p.getDeltaItem().get(3).getKey(),
                        p.getDeltaItem().get(4).getKey(),
                        p.getDeltaItem().get(5).getKey());
            }
        }
    }

    static void debug_read_data(long key, long version) {
        long andIncrement = LR.get();
        if (andIncrement == 0) {
            if (LR.getAndIncrement() == 0) {
                log("read data: {}, {}", key, version);
            }
        }
    }

    static void print_jvm_args() {
        long         mb         = 1024 * 1024;
        long         total      = Runtime.getRuntime().totalMemory() / mb;
        long         free       = Runtime.getRuntime().freeMemory() / mb;
        long         max        = Runtime.getRuntime().maxMemory() / mb;
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        List<String> inputArgs  = ManagementFactory.getRuntimeMXBean().getInputArguments();
        //        DebugUtil.info("Heap memory: {}", memoryBean.getHeapMemoryUsage());
        //        DebugUtil.info("Method memory: {}", memoryBean.getNonHeapMemoryUsage());
        log("Jvm arguments: {} ", String.valueOf(inputArgs));
        log("Total: {}, free: {}, max: {}", total, free, max);
    }
}
