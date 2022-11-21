package core.quantiles;


import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class QuantilesSketchTest {

    @Test
    public void updateDoublesSketchUseItCase() {
        Random rand = new Random();

        UpdateDoublesSketch sketch = DoublesSketch.builder().build(); // default k=128
        for (int i = 0; i < 10000; i++) {
            sketch.update(rand.nextGaussian());
        }

        System.out.println("Min, Median, Max values");
        System.out.println(Arrays.toString(sketch.getQuantiles(new double[]{0, 0.5, 1})));

        System.out.println("Probability Histogram: estimated probability mass in 4 bins: (-inf, -2), [-2, 0), [0, 2), [2, +inf)");
        System.out.println(Arrays.toString(sketch.getPMF(new double[]{-2, 0, 2})));

        System.out.println("Frequency Histogram: estimated number of original values in the same bins");
        double[] histogram = sketch.getPMF(new double[]{-2, 0, 2});
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] *= sketch.getN(); // scale the fractions by the total count of values
        }
        System.out.println(Arrays.toString(histogram));
    }

    @Test
    public void kllFloatsSketchUseItCase() {
        Random rand = new Random();
        KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
        for (int i = 0; i < 10000; i++) {
            sketch.update((float) rand.nextGaussian());
        }

        System.out.println("percentile");
        System.out.println(sketch.getQuantile(0.5));

        System.out.println("Min, Median, Max values");
        System.out.println(Arrays.toString(sketch.getQuantiles(new double[]{0, 0.5, 1})));
    }
}
