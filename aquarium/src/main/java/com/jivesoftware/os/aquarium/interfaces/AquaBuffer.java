package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 *
 * @author jonathan.colt
 */
public class AquaBuffer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public volatile ByteBuffer bb;
    public volatile byte[] bytes;
    public volatile int offset;
    public volatile int length = -1;

    public AquaBuffer() {
    }

    public Member toMember() {
        return new Member(copy());
    }

    public void force(ByteBuffer bb, int offset, int length) {
        this.bytes = null;
        this.bb = bb;
        this.offset = offset;
        this.length = length;
        if (offset + length > bb.limit()) {
            throw new IllegalArgumentException(bb + " cannot support offset=" + offset + " length=" + length);
        }
    }

    public void force(byte[] bytes, int offset, int length) {
        this.bb = null;
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        if (offset + length > bytes.length) {
            throw new IllegalArgumentException(bytes.length + " cannot support offset=" + offset + " length=" + length);
        }
    }

    public AquaBuffer(byte[] bytes) {
        this(bytes, 0, bytes == null ? -1 : bytes.length);
    }

    public AquaBuffer(byte[] bytes, int offet, int length) {
        this.bytes = bytes;
        this.offset = offet;
        this.length = length;
    }

    public byte get(int offset) {
        try {
            if (bb != null) {
                return bb.get(this.offset + offset);
            }
            return bytes[this.offset + offset];
        } catch (Exception x) {
            LOG.error("get({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public char getChar(int offset) {
        try {
            if (bb != null) {
                return bb.getChar(this.offset + offset);
            }
            return bytesChar(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("get({}) failed against{} ", offset, this);
            throw x;
        }
    }

    private static char bytesChar(byte[] bytes, int offset) {
        char v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public short getShort(int offset) {
        try {
            if (bb != null) {
                return bb.getShort(this.offset + offset);
            }
            return bytesShort(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    private static short bytesShort(byte[] bytes, int offset) {
        short v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public int getUnsignedShort(int offset) {
        try {
            if (bb != null) {
                return bb.getShort(this.offset + offset) & 0xffff;
            }
            return bytesUnsignedShort(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    private static int bytesUnsignedShort(byte[] bytes, int offset) {
        int v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public int getInt(int offset) {
        try {
            if (bb != null) {
                return bb.getInt(this.offset + offset);
            }
            return bytesInt(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    private static int bytesInt(byte[] bytes, int offset) {
        int v = 0;
        v |= (bytes[offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 3] & 0xFF);
        return v;
    }

    public long getUnsignedInt(int offset) {
        try {
            if (bb != null) {
                return bb.getInt(this.offset + offset) & 0xffffffffL;
            }
            return bytesInt(bytes, this.offset + offset) & 0xffffffffL;
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public long getLong(int offset) {
        try {
            if (bb != null) {
                return bb.getLong(this.offset + offset);
            }
            return bytesLong(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    private static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    public float getFloat(int offset) {
        try {
            if (bb != null) {
                return bb.getFloat(this.offset + offset);
            }
            return Float.intBitsToFloat(bytesInt(bytes, this.offset + offset));
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public double getDouble(int offset) {
        try {
            if (bb != null) {
                return bb.getDouble(this.offset + offset);
            }
            return Double.longBitsToDouble(bytesLong(bytes, this.offset + offset));
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public AquaBuffer sliceInto(int offset, int length, AquaBuffer aquaBuffer) {
        if (aquaBuffer == null || length == -1) {
            return null;
        }
        aquaBuffer.bb = bb;
        aquaBuffer.bytes = bytes;
        aquaBuffer.offset = this.offset + offset;
        aquaBuffer.length = length;
        return aquaBuffer;

    }

    public void allocate(int length) {
        if (length < 0) {
            throw new IllegalArgumentException(" allocate must be greater that or equal to zero. length=" + length);
        }
        if (bytes == null || bytes.length < length) {
            bb = null;
            bytes = new byte[length];
        }
        this.length = length;
    }

    public byte[] copy() {
        if (length == -1) {
            return null;
        }
        byte[] copy = new byte[length];
        if (bb != null) {
            for (int i = 0; i < length; i++) { // bb you suck.
                copy[i] = bb.get(offset + i);
            }
        } else {
            System.arraycopy(bytes, offset, copy, 0, length);
        }
        return copy;
    }

    public void set(AquaBuffer aquaBuffer) {
        allocate(aquaBuffer.length);
        offset = 0;
        length = aquaBuffer.length;
        if (aquaBuffer.bb != null) {
            for (int i = 0; i < aquaBuffer.length; i++) {
                bytes[i] = aquaBuffer.bb.get(aquaBuffer.offset + i);
            }
        } else {
            System.arraycopy(aquaBuffer.bytes, aquaBuffer.offset, bytes, 0, length);
        }
    }

    public void set(byte[] raw) {
        bb = null;
        bytes = raw;
        offset = 0;
        length = raw.length;
    }

    public LongBuffer asLongBuffer() {
        return asByteBuffer().asLongBuffer();
    }

    public ByteBuffer asByteBuffer() {
        if (length == -1) {
            return null;
        }
        if (bb != null) {
            ByteBuffer duplicate = bb.duplicate();
            duplicate.position(offset);
            duplicate.limit(offset + length);
            return duplicate.slice();
        }
        return ByteBuffer.wrap(copy());
    }

    public void get(int offset, byte[] copyInto, int o, int l) {
        if (bb != null) {
            for (int i = 0; i < copyInto.length; i++) {
                copyInto[o + i] = bb.get(o + i);
            }
        } else {
            System.arraycopy(bytes, offset, copyInto, o, l);
        }
    }



    @Override
    public int hashCode() {
        if (length == 0) {
            return 0;
        }

        if (bb != null) {
            int hash = 0;
            long randMult = 0x5DEECE66DL;
            long randAdd = 0xBL;
            long randMask = (1L << 48) - 1;
            long seed = bytes.length;

            for (int i = 0; i < length; i++) {
                long x = (seed * randMult + randAdd) & randMask;

                seed = x;
                hash += (bb.get(offset + i) + 128) * x;
            }

            return hash;
        }

        if (bytes != null) {
            int hash = 0;
            long randMult = 0x5DEECE66DL;
            long randAdd = 0xBL;
            long randMask = (1L << 48) - 1;
            long seed = bytes.length;

            for (int i = 0; i < length; i++) {
                long x = (seed * randMult + randAdd) & randMask;

                seed = x;
                hash += (bytes[offset + i] + 128) * x;
            }

            return hash;
        }
        return 0;

    }

    @Override
    public String toString() {
        return "aquaBuffer{" + "bb=" + bb + ", bytes=" + ((bytes == null) ? null : bytes.length) + ", offset=" + offset + ", length=" + length + '}';
    }

}
