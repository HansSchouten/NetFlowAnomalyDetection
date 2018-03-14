package org.tudelft.flink.streaming;

import java.util.HashMap;
import java.util.Set;

public class MisraGries {

    protected int k;
    protected int count;
    protected HashMap<String, Integer> counts = new HashMap<>();

    public MisraGries(int k) {
        this.k = k;
    }

    public void add(String element) {
        this.count++;
        if (this.counts.containsKey(element)) {
            this.counts.put(element, this.counts.get(element) + 1);
        } else {
            Set<String> keys = this.counts.keySet();
            if (keys.size() < this.k - 1) {
                this.counts.put(element, 1);
            } else {
                for (String key : keys) {
                    int newCount = this.counts.get(key) - 1;
                    if (newCount == 0) {
                        this.counts.remove(key);
                    } else {
                        this.counts.put(key, newCount);
                    }
                }
            }
        }
    }

    public String toString() {
        String res = "#flows : " + this.count + "\n";
        for (String key : this.counts.keySet()) {
            res += key + " : " + this.counts.get(key) + "\n";
        }
        return res;
    }

}
