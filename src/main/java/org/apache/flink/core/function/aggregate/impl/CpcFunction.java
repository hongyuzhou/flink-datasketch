package org.apache.flink.core.function.aggregate.impl;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;

import java.io.Serializable;

/** Internal function for summing up contents of fields. This is used with {@link SumAggregator}. */
@Internal
public abstract class CpcFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract void update(CpcSketch cpcSketch, Object o);

    public static CpcFunction getForClass(Class<?> clazz) {

        if (clazz == Long.class) {
            return new LongCpc();
        } else if (clazz == Double.class) {
            return new DoubleCpc();
        } else if (clazz == String.class) {
            return new StringCpc();
        } else if (clazz == int[].class) {
            return new IntArrayCpc();
        } else if (clazz == long[].class) {
            return new LongArrayCpc();
        } else if (clazz == char[].class) {
            return new CharArrayCpc();
        } else if (clazz == byte[].class) {
            return new ByteArrayCpc();
        } else {
            throw new RuntimeException(
                    "DataStream cannot be cpcSketch because the class "
                            + clazz.getSimpleName()
                            + " does not support the + operator.");
        }
    }

    static class LongCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((long) o);
        }
    }

    static class DoubleCpc extends CpcFunction {

        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((double) o);
        }
    }

    static class StringCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((String) o);
        }
    }

    static class IntArrayCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((int[]) o);
        }
    }

    static class LongArrayCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((long[]) o);
        }
    }

    static class CharArrayCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((char[]) o);
        }
    }

    static class ByteArrayCpc extends CpcFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void update(CpcSketch cpcSketch, Object o) {
            cpcSketch.update((byte[]) o);
        }
    }


}
