package org.apache.flink.function.aggregate.impl;

import org.apache.datasketches.hll.HllSketch;

import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class HllFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract void update(HllSketch hllSketch, Object o);

    public static HllFunction getForClass(Class<?> clazz) {

        if (clazz == Long.class) {
            return new LongHll();
        } else if (clazz == Double.class) {
            return new DoubleHll();
        } else if (clazz == String.class) {
            return new StringHll();
        } else if (clazz == ByteBuffer.class) {
            return new ByteBufferHll();
        } else if (clazz == int[].class) {
            return new IntArrayHll();
        } else if (clazz == long[].class) {
            return new LongArrayHll();
        } else if (clazz == char[].class) {
            return new CharArrayHll();
        } else if (clazz == byte[].class) {
            return new ByteArrayHll();
        } else {
            throw new RuntimeException(
                    "DataStream cannot be HllSketch because the class "
                            + clazz.getSimpleName()
                            + " does not support the + operator.");
        }
    }

    static class LongHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((long) o);
        }
    }

    static class DoubleHll extends HllFunction {

        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((double) o);
        }
    }

    static class StringHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((String) o);
        }
    }

    static class ByteBufferHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((ByteBuffer) o);
        }
    }

    static class IntArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((int[]) o);
        }
    }

    static class LongArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((long[]) o);
        }
    }

    static class CharArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((char[]) o);
        }
    }

    static class ByteArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(HllSketch hllSketch, Object o) {
            hllSketch.update((byte[]) o);
        }
    }


}
