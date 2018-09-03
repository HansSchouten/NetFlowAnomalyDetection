package org.tudelft.flink.streaming.statemachines.helpers;

import org.tudelft.flink.streaming.statemachines.StateMachineNetFlow;
import org.tudelft.flink.streaming.statemachines.Symbol;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class PatternTester {

    protected List<Symbol> currentPattern;
    protected int currentIndex;

    /**
     * PatternTester constructor.
     */
    public PatternTester() {
        List<Integer> pattern = Arrays.asList(
            0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1
        );
        try {
            pattern = readPattern();
        } catch (Exception ex) {
            System.out.println("Reading pattern error: " + ex.getMessage());
        }

        setPattern(pattern);
    }

    protected ArrayList<Integer> readPattern() throws FileNotFoundException {
        String path = "input\\pautomac\\set-I\\0-1pautomac.train";
        File file = new File(path);
        Scanner sc = new Scanner(file);
        // skip first line
        sc.nextLine();
        ArrayList<Integer> allSymbols = new ArrayList<>();
        while (sc.hasNextLine()) {
            // read line
            String sequence = sc.nextLine();
            // add all symbols
            String[] symbols = sequence.split(" ");
            for (int i = 1; i < symbols.length; i++) {
                int symbol = Integer.valueOf(symbols[i]);
                allSymbols.add(symbol);
            }

            // fill up with -1
            for (int i = 0; i < StateMachineNetFlow.FUTURE_SIZE; i++) {
                allSymbols.add(-1);
            }
        }

        System.out.println("Pattern size: " + allSymbols.size());
        return allSymbols;
    }

    /**
     * Convert a pattern of integers in a pattern of Symbols.
     *
     * @param pattern
     */
    public void setPattern(List<Integer> pattern) {
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
