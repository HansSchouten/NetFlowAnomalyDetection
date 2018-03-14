package org.tudelft.flink.streaming;

import java.util.PriorityQueue;

public class TopN {

    public PriorityQueue<PrioEntry> queue;
    protected int max_size;

    /**
     * Construct a new TopN
     * @param max_size      the maximum number of elements allowed in this TopN.
     */
    public TopN(int max_size) {
        this.queue = new PriorityQueue<>();
        this.max_size = max_size;
    }

    /**
     * Update the value corresponding with the given pattern by the given count.
     *
     * If the value is not contained in the TopN, it is added on two conditions:
     *   1. the TopN is less than the provided max size
     *   2. or, the given value is higher than the lowest value stored in TopN
     *
     * @param pattern
     * @param count
     */
    public void update(String pattern, long count) {
        PrioEntry entry = new PrioEntry(pattern, count);

        // if pattern already in priority queue, re-add to update count
        if (this.queue.contains(entry)) {
            this.queue.remove(entry);
            this.queue.add(entry);
            return;
        }

        PrioEntry lowestEntry = this.queue.peek();
        // if queue is empty, add entry
        if (lowestEntry == null) {
            this.queue.add(entry);
        }
        else {
            // if queue is not at max size yet, add entry
            if (this.queue.size() < this.max_size) {
                this.queue.add(entry);
            } else {
                // if count of new entry is higher than lowest value in queue, replace
                if (count > lowestEntry.getValue()) {
                    this.queue.remove(lowestEntry);
                    this.queue.add(entry);
                }
            }
        }
    }

    /**
     * Merge this TopN with the given TopN.
     * @param other
     */
    public void merge(TopN other) {
        while (other.queue.size() > 0) {
            PrioEntry entry = other.queue.remove();
            this.update(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Return the number of elements contained in this TopN.
     * @return
     */
    public int size() {
        return this.queue.size();
    }

    /**
     * Return the string representation of this TopN.
     * @return
     */
    public String toString() {
        String patterns = "";
        while (this.queue.size() > 0) {
            PrioEntry entry = this.queue.remove();
            patterns = entry.getKey() + ": " + entry.getValue() + "\n" + patterns;
        }
        return patterns;
    }

}
