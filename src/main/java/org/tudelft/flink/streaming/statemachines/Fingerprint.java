package org.tudelft.flink.streaming.statemachines;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Fingerprint {

    protected List<List<Symbol>> traces;
    protected List<Double> probabilities;
    protected String path;
    protected String name;
    protected Integer step;
    protected boolean isLoaded = false;

    protected double previousDistance;
    public static double MATCH_THRESHOLD = 1.0;
    public static double MIN_PROBABILITY = 0.000001;

    /**
     * Fingerprint constructor.
     *
     * @param path
     * @param name
     * @param step
     */
    public Fingerprint(String path, String name, Integer step) {
        this.path = path;
        this.name = name;
        this.step = step;
    }

    /**
     * Load a fingerprint from file.
     *
     * @throws Exception
     */
    public void loadFingerprint() throws Exception {
        this.probabilities = new ArrayList<>();
        this.traces = new ArrayList<>();
        this.isLoaded = true;

        Scanner reader = new Scanner(new BufferedReader(new FileReader(path)));
        while (reader.hasNextLine()) {
            String line = reader.nextLine();
            String[] parts = line.split(" ");
            List<Symbol> sequence = new ArrayList<>();

            int length = Integer.parseInt(parts[0]);
            for (int i = 1; i <= length; i++) {
                Symbol symbol = new Symbol(parts[i]);
                sequence.add(symbol);
            }

            double probability = Double.parseDouble(parts[length + 1]);
            probability = Math.max(probability, MIN_PROBABILITY);
            this.traces.add(sequence);
            this.probabilities.add(probability);
        }
    }

    /**
     * Return whether the State Machine starting with the given root state matches this fingerprint.
     *
     * @param root
     * @param stateMachineID
     * @return
     */
    public boolean match(State root, String stateMachineID) {
        if (! this.isLoaded) {
            try {
                loadFingerprint();
            } catch (Exception ex) {
                System.out.println("Error while loading: " + this.path);
            }
        }

        List<Double> otherProbabilities = getProbabilities(root);
        this.previousDistance = KLDivergence(this.probabilities, otherProbabilities);
        boolean match = this.previousDistance <= MATCH_THRESHOLD;
        if (match) {
            System.out.println(stateMachineID + " matches with " + this.getName() + "! KL Distance: " + this.previousDistance);
        }
        return match;
    }

    /**
     * Return a list of probabilities for the traces of this fingerprint in the given State Machine.
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
                State nextState = state.getState(symbol, null);
                if (nextState.getColor() != State.Color.RED) {
                    chance = 0;
                    break;
                }
                Double transitionProbability = state.getTransitionProbability(symbol);
                chance *= transitionProbability;
                state = nextState;
            }
            chance = Math.max(chance, MIN_PROBABILITY);

            probabilities.add(chance);
        }

        return probabilities;
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
     * Return the name of the behaviour captured in this fingerprint.
     *
     * @return
     */
    public String getName() {
        return this.name + " - Step " + this.step;
    }

}
