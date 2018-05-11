package org.tudelft.flink.streaming.statemachines.helpers;

import org.tudelft.flink.streaming.statemachines.Symbol;

import java.util.ArrayList;
import java.util.List;

public class PatternTester {

    protected List<Symbol> currentPattern;
    protected int currentIndex;

    /**
     * PatternTester constructor.
     */
    public PatternTester() {
        int[] pattern = new int[] {
            0,1
        };
        setPattern(pattern);
    }

    /**
     * Convert a pattern of integers in a pattern of Symbols.
     *
     * @param pattern
     */
    public void setPattern(int[] pattern) {
        this.currentPattern = new ArrayList<>();
        for (int num : pattern) {
            Symbol symbol = new Symbol(Integer.toString(num));
            this.currentPattern.add(symbol);
        }
        this.currentIndex = 0;
    }

    /**
     * Return the next symbol from the predefined pattern.
     *
     * @return
     */
    public Symbol getNext() {
        Symbol next = this.currentPattern.get(this.currentIndex);
        this.currentIndex++;
        if (this.currentIndex == this.currentPattern.size()) {
            this.currentIndex = 0;
        }
        return next;
    }

}
