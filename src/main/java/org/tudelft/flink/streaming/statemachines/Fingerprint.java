package org.tudelft.flink.streaming.statemachines;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Fingerprint {

    protected Map<List<Symbol>, Double> traces;
    protected List<Double> probabilities;
    protected String name;

    protected double previousDistance;
    public static double MATCH_THRESHOLD = 0.2;
    public static double MIN_PROBABILITY = 0.000001;

    /**
     * Load a fingerprint from file.
     *
     * @param path
     * @param name
     * @throws Exception
     */
    public void loadFingerprint(String path, String name) throws Exception {
        this.probabilities = new ArrayList<>();
        this.traces = new HashMap<>();
        this.name = name;

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
            this.traces.put(sequence, probability);
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
        List<Double> otherProbabilities = getProbabilities(root);
        this.previousDistance = KLDivergence(this.probabilities, otherProbabilities);
        boolean match = this.previousDistance <= MATCH_THRESHOLD;

        if (this.previousDistance < 3) {
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
        for (List<Symbol> sequence : this.traces.keySet()) {
            double chance = 1;

            State state = root;
            for (Symbol symbol : sequence) {
                if (! state.hasTransition(symbol)) {
                    chance = 0;
                    break;
                }
                chance *= state.getTransitionProbability(symbol);
                state = state.getState(symbol, null);
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
        //System.out.println();
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
        return this.name;
    }

}
