package org.tudelft.flink.streaming.statemachines.validation;

import org.tudelft.flink.streaming.statemachines.State;
import org.tudelft.flink.streaming.statemachines.Symbol;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class PautomacValidator {

    List<List<Symbol>> traces;
    List<Double> probabilities;
    Set<Integer> skipIndices;
    public static double MIN_PROBABILITY = 0.000000001;

    public PautomacValidator(String path) {
        skipIndices = new HashSet<>();
        try {
            readTestSet(path);
            readSolution(path);
        } catch (Exception ex) {
            System.out.println("Reading pattern error: " + ex.getMessage());
        }
    }

    public void readTestSet(String path) throws FileNotFoundException {
        File file = new File(path + ".test");
        Scanner sc = new Scanner(file);
        sc.nextLine();

        this.traces = new ArrayList<>();
        int c = 0;
        while (sc.hasNextLine()) {
            c++;
            List<Symbol> sequence = new ArrayList<>();
            String line = sc.nextLine();
            String[] parts = line.split(" ");
            for (int i = 1; i < parts.length; i++) {
                Symbol symbol = new Symbol(parts[i]);
                sequence.add(symbol);
            }
            if (sequence.size() == 0) {
                skipIndices.add(c);
            } else {
                traces.add(sequence);
            }
        }
    }

    public void readSolution(String path) throws FileNotFoundException {
        File file = new File(path+ "_solution.txt");
        Scanner sc = new Scanner(file);
        sc.nextLine();

        this.probabilities = new ArrayList<>();
        int c = 0;
        while (sc.hasNextLine()) {
            c++;
            String probability = sc.nextLine();
            if (skipIndices.contains(c)) {
                continue;
            }
            this.probabilities.add(Double.valueOf(probability));
        }
    }

    public double getDistance(State root) {
        List<Double> otherProbabilities = getProbabilities(root);
        return KLDivergence(normalize(this.probabilities), normalize(otherProbabilities));
    }

    /**
     * Return the KL Divergence between two distributions.
     *
     * @param fingerprintsProbs
     * @param statemachineProbs
     * @return
     */
    public double KLDivergence(List<Double> fingerprintsProbs, List<Double> statemachineProbs) {
        //System.out.println("==========================");
        //System.out.println("Computing KL-Divergence...");
        //System.out.println("--------------------------");

        double sum = 0;
        for (int i = 0; i < fingerprintsProbs.size(); i++) {
            //System.out.println(fingerprintsProbs.get(i) + " " + statemachineProbs.get(i));
            sum += fingerprintsProbs.get(i) * Math.log(fingerprintsProbs.get(i) / statemachineProbs.get(i));
        }
        //System.out.println("--------------------------");
        //System.out.println("KL-Divergence: " + sum);

        return sum;
    }

    /**
     * Return a list of probabilities for the traces of the test set in the given State Machine.
     *
     * @param root
     * @return
     */
    public List<Double> getProbabilities(State root) {
        List<Double> probabilities = new ArrayList<>();

        // loop through all traces of this fingerprint
        for (List<Symbol> sequence : this.traces) {
            double chance = 1;

            State state = root;
            for (Symbol symbol : sequence) {
                if (! state.hasTransition(symbol)) {
                    chance = 0;
                    break;
                }
                State nextState = state.getState(symbol);
                if (nextState.getColor() != State.Color.RED) {
                    chance = 0;
                    break;
                }
                Double transitionProbability = 0.0;
                try {
                    transitionProbability = state.getTransitionProbability(symbol);
                } catch (Exception ex) {
                }
                chance *= transitionProbability;
                state = nextState;
            }
            chance = Math.max(chance, MIN_PROBABILITY);

            probabilities.add(chance);
        }

        return probabilities;
    }

    public List<Double> normalize(List<Double> vector) {
        double sum = 0.0;
        for (Double value : vector) {
            sum += value;
        }
        List<Double> normalized = new ArrayList<>();
        for (Double value : vector) {
            normalized.add(value / sum);
        }
        return normalized;
    }

}
