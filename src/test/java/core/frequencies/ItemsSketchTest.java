package core.frequencies;


import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.junit.Test;

public class ItemsSketchTest {

    @Test
    public void itemsSketchUseItCase() {
        ItemsSketch<String> sketch = new ItemsSketch<>(64);
        sketch.update("a");
        sketch.update("a");
        sketch.update("b");
        sketch.update("c");
        sketch.update("a");
        sketch.update("d");
        sketch.update("a");

        System.out.println("a items: " + sketch.getEstimate("a"));

        ItemsSketch.Row<String>[] items = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        System.out.println("Frequent items: " + items.length);
        System.out.println(ItemsSketch.Row.getRowHeader());
        for (ItemsSketch.Row<String> row : items) {
            System.out.println(row.toString());
        }
    }

    @Test
    public void itemsUnionSketchUseItCase() {
        ItemsSketch<String> sketch1 = new ItemsSketch<>(64);
        sketch1.update("a");
        sketch1.update("a");
        sketch1.update("b");
        sketch1.update("c");
        sketch1.update("a");
        sketch1.update("d");
        sketch1.update("a");

        ItemsSketch<String> sketch2 = new ItemsSketch<>(64);
        sketch2.update("e");
        sketch2.update("a");
        sketch2.update("f");
        sketch2.update("f");
        sketch2.update("f");
        sketch2.update("g");
        sketch2.update("a");
        sketch2.update("f");

        ItemsSketch<String> union = new ItemsSketch<>(64);
        union.merge(sketch1);
        union.merge(sketch2);

        ItemsSketch.Row<String>[] items = union.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        System.out.println("Frequent items: " + items.length);
        System.out.println(ItemsSketch.Row.getRowHeader());
        for (ItemsSketch.Row<String> row : items) {
            System.out.println(row.toString());
        }
    }
}
