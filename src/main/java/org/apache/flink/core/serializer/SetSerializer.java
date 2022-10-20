package org.apache.flink.core.serializer;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A serializer for {@link Set}. The serializer relies on a key serializer
 *
 * @param <K> The type of the keys in the Set.
 */
public class SetSerializer<K> extends TypeSerializer<Set<K>> {

    private final TypeSerializer<K> keySerializer;

    public SetSerializer(TypeSerializer<K> keySerializer) {
        this.keySerializer =
                Preconditions.checkNotNull(keySerializer, "The key serializer cannot be null");
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<K>> duplicate() {
        TypeSerializer<K> duplicateKeySerializer = keySerializer.duplicate();
        return (duplicateKeySerializer == keySerializer) ? this : new SetSerializer<>(duplicateKeySerializer);
    }

    @Override
    public Set<K> createInstance() {
        return new HashSet<>();
    }

    @Override
    public Set<K> copy(Set<K> from) {
        Set<K> newSet = new HashSet<>();
        for (K k : from) {
            K newKey = keySerializer.copy(k);
            newSet.add(newKey);
        }
        return newSet;
    }

    @Override
    public Set<K> copy(Set<K> from, Set<K> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Set<K> record, DataOutputView target) throws IOException {
        final int size = record.size();
        target.writeInt(size);

        for (K k : record) {
            keySerializer.serialize(k, target);
        }
    }

    @Override
    public Set<K> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();

        final Set<K> set = new HashSet<>(size);
        for (int i = 0; i < size; ++i) {
            K key = keySerializer.deserialize(source);
            set.add(key);
        }

        return set;
    }

    @Override
    public Set<K> deserialize(Set<K> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int size = source.readInt();
        target.writeInt(size);

        for (int i = 0; i < size; ++i) {
            keySerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                && obj.getClass() == getClass()
                && keySerializer.equals(((SetSerializer<?>) obj).getKeySerializer()));
    }

    @Override
    public int hashCode() {
        return keySerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Set<K>> snapshotConfiguration() {
        return new SetSerializerSnapshot<>(this);
    }

    public final static class SetSerializerSnapshot<KEY> extends CompositeTypeSerializerSnapshot<Set<KEY>, SetSerializer<KEY>> {

        private static final int CURRENT_VERSION = 1;

        public SetSerializerSnapshot() {
            super(SetSerializer.class);
        }

        public SetSerializerSnapshot(SetSerializer<KEY> kSetSerializer) {
            super(kSetSerializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(SetSerializer<KEY> outerSerializer) {
            return new TypeSerializer<?>[]{outerSerializer.getKeySerializer()};
        }

        @Override
        protected SetSerializer<KEY> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<KEY> keySerializer = (TypeSerializer<KEY>) nestedSerializers[0];
            return new SetSerializer<>(keySerializer);
        }
    }
}
