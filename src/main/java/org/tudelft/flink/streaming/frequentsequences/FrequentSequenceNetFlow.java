package org.tudelft.flink.streaming.frequentsequences;

import javafx.util.Pair;
import org.tudelft.flink.streaming.NetFlow;

import java.util.HashMap;

public class FrequentSequenceNetFlow extends NetFlow {

    /**
     * An enumeration containing all possible states.
     */
    public enum State {
        LOW_SIZE, MEDIUM_SIZE, HIGH_SIZE
    }

    /**
     * The state of the previously consumed NetFlow
     */
    public State previous;
    /**
     * The exact count of each state transition.
     */
    public HashMap<Pair, Long> transitionCounts;


    public FrequentSequenceNetFlow() {
        this.transitionCounts = new HashMap<>();
    }

    public void setFromString(String line) {
        super.setFromString(line);
    }


    /**
     * Consume a new NetFlow and update rolling state parameters.
     *
     * @param nextNetFlow
     */
    public void processFlow(FrequentSequenceNetFlow nextNetFlow) {
        // on consuming the first NetFlow, process this (the rolling NetFlow object)
        if (this.previous == null) {
            this.previous = getState(this);
        }

        // process the incoming NetFlow
        State next = getState(nextNetFlow);
        updateCount(this.previous, next);
        this.previous = next;
    }

    /**
     * Increase the count for the given state transition, or add the state transition if this is the first occurrence.
     *
     * @param previous
     * @param next
     */
    protected void updateCount(State previous, State next) {
        Pair<State, State> transition = new Pair<>(previous, next);
        if (this.transitionCounts.containsKey(transition)) {
            this.transitionCounts.put(transition, this.transitionCounts.get(transition) + 1);
        } else {
            this.transitionCounts.put(transition, 1L);
        }
    }


    /**
     * Return the current state based on the given NetFlow.
     *
     * @param netFlow       the NetFlow for which the state will be determined
     * @return
     */
    protected State getState(FrequentSequenceNetFlow netFlow) {
        if (netFlow.flow_size < 200) {
            return State.LOW_SIZE;
        }
        if (netFlow.flow_size < 300) {
            return State.MEDIUM_SIZE;
        }
        return State.HIGH_SIZE;
    }


    @Override
    public String toString() {
        String res = "Transition counts for " + this.srcIP + "\n";
        for (Pair<State, State> transition : this.transitionCounts.keySet()) {
            Long count = this.transitionCounts.get(transition);
            res += transition.getKey() + " > " + transition.getValue() + " : " + count + "\n";
        }
        res += "\nTransition probabilities for " + this.srcIP + "\n";
        for (State from : State.values()) {
            // compute the sum of the transition occurrences to other states
            long sum = 0;
            for (State to : State.values()) {
                Pair<State, State> transition = new Pair<>(from, to);
                if (this.transitionCounts.containsKey(transition)) {
                    sum += this.transitionCounts.get(transition);
                }
            }
            // compute the transition probabilities to other states
            for (State to : State.values()) {
                Pair<State, State> transition = new Pair<>(from, to);
                if (this.transitionCounts.containsKey(transition)) {
                    // compute transition probability with two decimals precision
                    double chance = Math.round(this.transitionCounts.get(transition) * 100.0 / sum) / 100.0;
                    res += from + " > " + to + " : " + chance + "\n";
                }
            }
        }
        return res;
    }

}