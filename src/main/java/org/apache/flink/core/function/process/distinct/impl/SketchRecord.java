package org.apache.flink.core.function.process.distinct.impl;

import java.util.Objects;

public class SketchRecord<T> {

    private T value;

    private Double estimate = 0.0d;

    public SketchRecord(T value) {
        this.value = value;
    }

    public SketchRecord(T value, Double estimate) {
        this.value = value;
        this.estimate = estimate;
    }

    public T getValue() {
        return value;
    }

    public Double getEstimate() {
        return estimate;
    }

    public void setEstimate(Double estimate) {
        this.estimate = estimate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SketchRecord<?> that = (SketchRecord<?>) o;
        return value.equals(that.value) && estimate.equals(that.estimate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, estimate);
    }

    @Override
    public String toString() {
        return "SketchRecord{" +
                "value=" + value +
                ", estimate=" + estimate +
                '}';
    }
}
