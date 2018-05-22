package org.tudelft.flink.streaming.statemachines.validation;

import org.tudelft.flink.streaming.statemachines.visualisation.PAutomacVisualiser;

import java.io.File;
import java.util.*;

public class VisualisePAutomac {

    /**
     * All states of the Finite State Machine.
     */
    protected List<Integer> states;
    /**
     * Initial state.
     */
    protected int initial;
    /**
     * A list of all transitions with for each transition a list with the values: from, symbol, to
     */
    protected List<List<Integer>> transitions;


    /**
     * Visualise the State Machine described by the model stored in the given file.
     *
     * @param path
     * @throws Exception
     */
    public void visualise(String path) throws Exception {
        // init variables
        this.states = new ArrayList<>();
        this.transitions = new ArrayList<>();

        // read model file
        readFile(path);

        // visualise State Machine
        visualiseAutomata();
    }

    /**
     * Read the given model file and set all variables in this object instance.
     *
     * @param path
     * @throws Exception
     */
    public void readFile(String path) throws Exception {
        File file = new File(path);
        Scanner sc = new Scanner(file);

        // skip first line
        sc.nextLine();
        // read initial state
        double[] values = parseLine(sc.nextLine());
        this.initial = (int) values[0];

        // skip line
        sc.nextLine();
        // read end states
        while(true) {
            String line = sc.nextLine();
            // stop criteria
            if (line.startsWith("S")) {
                break;
            }

            values = parseLine(line);
        }

        // read state,symbol combinations
        while(true) {
            String line = sc.nextLine();
            // stop criteria
            if (line.startsWith("T")) {
                break;
            }

            values = parseLine(line);
            this.states.add((int) values[0]);
        }

        // read transitions
        while(sc.hasNextLine()) {
            String line = sc.nextLine();

            values = parseLine(line);
            List<Integer> transition = new ArrayList<>();
            transition.add((int) values[0]);
            transition.add((int) values[1]);
            transition.add((int) values[2]);
            this.transitions.add(transition);
        }

        sc.close();
    }

    /**
     * Parse a raw line of the file into the values it contains.
     *
     * @param line
     * @return
     */
    public double[] parseLine(String line) {
        // split on space
        String[] parts = line.split(" ");
        // remove leading tab
        String part1 = parts[0].trim();
        // remove ( and )
        String valuesString = part1.substring(1, part1.length() - 1);
        // get all values divided by ,
        String[] valuesStrings = valuesString.split(",");

        // define result size and init array
        int result_length = valuesStrings.length + 1;
        double[] result = new double[result_length];

        // add values between brackets
        for (int i = 0; i < result_length - 1; i++) {
            result[i] = Double.valueOf(valuesStrings[i]);
        }
        // add probability
        result[result_length - 1] = Double.valueOf(parts[1]);
        // return result
        return result;
    }

    /**
     * Visualise the State Machine based on the parsed model.
     */
    public void visualiseAutomata() {
        PAutomacVisualiser visualiser = new PAutomacVisualiser();
        visualiser.visualise(this.states, this.initial, this.transitions);
    }


    /**
     * Print the values of the given array (for debugging purposes).
     *
     * @param items
     */
    protected void printArray(double[] items) {
        for (double item : items) {
            System.out.println(item);
        }
    }

}