package org.tudelft.flink.streaming.statemachines.helpers;

import org.tudelft.flink.streaming.statemachines.Symbol;

import java.io.*;
import java.util.*;

public class PatternFileOutput {

    protected int future_size;
    protected List<String> patterns;

    /**
     * PatternFileOutput constructor.
     */
    public PatternFileOutput(int future_size) {
        this.future_size = future_size;
        this.patterns = new LinkedList<>();
    }

    /**
     * Add the given pattern to the list of all encountered patterns.
     *
     * @param pattern
     */
    public void addPattern(Queue<Symbol> pattern) {
        String patternString = "";
        for (Symbol transition : pattern) {
            patternString += transition.toString() + " ";
        }
        this.patterns.add(patternString);
    }

    /**
     * Write the encountered patterns to a file.
     *
     * @param stateMachineID
     */
    public void writeToFile(String stateMachineID) {
        String cleanID = stateMachineID.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
        String path = "output\\state-machines\\trace-" + cleanID + ".txt";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            // write number of patterns and the alphabet size
            int sequence_count = this.patterns.size();
            int alphabet_size = SymbolConfig.alphabet_size;
            writer.write(sequence_count + ", " + alphabet_size + "\n");

            // write each encountered pattern
            for (String pattern : this.patterns) {
                writer.write(this.future_size + " " + pattern + "\n");
            }
            writer.close();
        }
        catch(IOException ex) {
            System.out.println("Error writing Pattern File:\n" + ex.getMessage());
        }
    }

}
