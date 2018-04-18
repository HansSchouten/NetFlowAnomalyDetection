package org.tudelft.flink.streaming.statemachines;

public class Symbol {

    protected String symbol;

    public Symbol(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return this.symbol;
    }

    @Override
    public int hashCode() {
        return this.symbol.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return this.symbol.equals(other.toString());
    }

}