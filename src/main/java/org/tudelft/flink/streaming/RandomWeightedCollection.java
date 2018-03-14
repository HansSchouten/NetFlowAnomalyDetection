package org.tudelft.flink.streaming;

import java.io.Serializable;
import java.util.Random;
import java.util.TreeMap;

public class RandomWeightedCollection implements Serializable {

    private static final long serialVersionUID = 8078274017619893052L;

    private final TreeMap<Double, String> map = new TreeMap<>();
    private final Random random;
    private double total = 0;

    public RandomWeightedCollection() {
        this(new Random());
    }

    public RandomWeightedCollection(Random random) {
        this.random = random;
    }

    public void add(double weight, String element) {
        total += weight;
        map.put(total, element);
    }

    public String randomEntry() {
        double value = random.nextDouble() * total;
        return map.higherEntry(value).getValue();
    }
}