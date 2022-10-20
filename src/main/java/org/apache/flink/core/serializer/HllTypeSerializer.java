package org.apache.flink.core.serializer;

import org.apache.datasketches.hll.HllSketch;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A serializer for {@link HllSketch}
 * HllTypeSerializer
 */
public class HllTypeSerializer extends TypeSerializerSingleton<HllSketch> {

    private static final long serialVersionUID = 1L;

    private static final HllSketch EMPTY = new HllSketch();

    public static final HllTypeSerializer INSTANCE = new HllTypeSerializer();

    public HllTypeSerializer() {
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public HllSketch createInstance() {
        return EMPTY;
    }

    @Override
    public HllSketch copy(HllSketch from) {
        return from.copy();
    }

    @Override
    public HllSketch copy(HllSketch from, HllSketch reuse) {
        return this.copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(HllSketch record, DataOutputView target) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("The record must not be null.");
        } else {
            byte[] bytes = record.toCompactByteArray();
            int len = bytes.length;
            target.writeInt(len);
            target.write(bytes);
        }
    }

    @Override
    public HllSketch deserialize(DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] result = new byte[len];
        source.readFully(result);
        return HllSketch.heapify(result);
    }

    @Override
    public HllSketch deserialize(HllSketch reuse, DataInputView source) throws IOException {
        return this.deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        target.writeInt(len);
        target.write(source, len);
    }

    @Override
    public TypeSerializerSnapshot<HllSketch> snapshotConfiguration() {
        return new HllTypeSerializerSnapshot();
    }

    public static final class HllTypeSerializerSnapshot extends SimpleTypeSerializerSnapshot<HllSketch> {
        public HllTypeSerializerSnapshot() {
            super(() -> HllTypeSerializer.INSTANCE);
        }
    }


}
