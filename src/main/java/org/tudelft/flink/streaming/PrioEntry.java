package org.tudelft.flink.streaming;

public class PrioEntry implements Comparable<PrioEntry> {
    protected String key;
    protected long value;

    public PrioEntry(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public long getValue() {
        return this.value;
    }

    public String getKey() {
        return this.key;
    }

    @Override
    public int compareTo(PrioEntry other) {
        if (this.getValue() < other.getValue()) {
            return -1;
        } else if(this.getValue() > other.getValue()) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj){
            return true;
        }
        if (obj == null){
            return false;
        }
        if (getClass() != obj.getClass()){
            return false;
        }
        PrioEntry other = (PrioEntry) obj;
        return this.getKey().equals(other.getKey());
    }
}
