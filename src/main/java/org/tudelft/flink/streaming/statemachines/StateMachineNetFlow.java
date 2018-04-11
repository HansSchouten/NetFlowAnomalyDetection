package org.tudelft.flink.streaming.statemachines;

import org.tudelft.flink.streaming.NetFlow;

import java.util.*;

public class StateMachineNetFlow extends NetFlow {

    /**
     * Number of symbols contained in a future.
     */
    public static final int FUTURE_SIZE = 3;
    /**
     * Visualise final State Machine.
     */
    public static final boolean VISUALISE = true;

    /**
     * The root node of the State Machine.
     */
    public State root;
    /**
     * The symbol of the consumed NetFlow.
     */
    public Symbol currentSymbol;
    /**
     * The symbols that represent the chain of future states, starting from the current state.
     */
    public Queue<Symbol> future;
    /**
     * The list of states in which the current future is evaluated (increasing the occurrence frequency of this future).
     */
    public Map<Integer, State> instances;
    /**
     * The list of all red states (states that are part of the final State Machine).
     */
    public List<State> redStates;
    /**
     * If set, a predefined pattern will be used instead of the Symbols extracted from the incoming NetFlows.
     */
    public PatternTester patternTester;

    /**
     * StateMachineNetFlow constructor.
     */
    public StateMachineNetFlow() {
        this.instances = new HashMap<>();
        this.future = new LinkedList<>();
        this.redStates = new LinkedList<>();

        this.root = new State(State.Color.RED, 0);
        this.redStates.add(this.root);
    }

    /**
     * Set NetFlow parameters from the given input string.
     *
     * @param line
     */
    public void setFromString(String line) {
        super.setFromString(line);
    }

    /**
     * Consume a new NetFlow and update rolling state parameters.
     *
     * @param nextNetFlow
     */
    public void processFlow(StateMachineNetFlow nextNetFlow) {
        // get the symbol of the incoming NetFlow
        this.currentSymbol = getSymbol(nextNetFlow);

        // update the future
        if (this.future.size() == FUTURE_SIZE) {
            this.future.poll();
        }
        this.future.add(this.currentSymbol);

        // all futures start in the root, so add the root to the list of states to evaluate the current future on
        if (this.future.size() == FUTURE_SIZE) {
            instances.put(this.root.hashCode(), this.root);
        }

        // evaluate this future in all state instances (increasing the occurrence frequency of this future)
        evaluateInstances();
    }

    /**
     * Increase the occurrence frequency of the current future in the instance states and move each instance to the next state.
     */
    protected void evaluateInstances() {
        Map<Integer, State> newInstances = new HashMap<>();

        for (Integer instanceKey : this.instances.keySet()) {
            // get instance state
            State instance = this.instances.get(instanceKey);

            // increase occurrence frequency of this future in this state
            instance.increaseFrequency(this.future);

            // merge or color the state if a significant amount of futures are captured in this state
            if (instance.isSignificant()) {
                boolean is_merged = false;
                for (State redState : this.redStates) {
                    if (instance.similarTo(redState)) {
                        // merge instance with red state (pointing all incoming transitions to the red state instead)
                        instance.merge(redState);
                        // continue evaluation in the red state the instance has merged with
                        instance = redState;
                        is_merged = true;
                        break;
                    }
                }
                // color state red if it could not be merged with any other red state
                if (! is_merged) {
                    instance.changeToRed();
                    this.redStates.add(instance);
                }
            }

            // get the next state in the direction of the symbol of the consumed NetFlow
            State next = instance.getState(this.currentSymbol);

            // if a state in the direction of the current symbol exists, add the next state to instances
            if (next != null) {
                newInstances.put(next.hashCode(), next);
            }
        }

        this.instances = newInstances;
    }

    /**
     * Return the current transition symbol based on the parameters of the given NetFlow.
     *
     * @param netFlow       the NetFlow for which the transition symbol will be determined
     * @return
     */
    protected Symbol getSymbol(StateMachineNetFlow netFlow) {
        // if a pattern tester is set, return the next symbol from the predefined pattern
        if (this.patternTester != null) {
            return this.patternTester.getNext();
        }

        if (netFlow.byteCount < 200) {
            return Symbol.LOW_SIZE;
        }
        if (netFlow.byteCount < 300) {
            return Symbol.MEDIUM_SIZE;
        }
        return Symbol.HIGH_SIZE;
    }

    /**
     * Return the string representation of this rolling StateMachineNetFlow.
     *
     * @return
     */
    @Override
    public String toString() {
        if (VISUALISE) {
            StateMachineVisualiser visualiser = new StateMachineVisualiser();
            if (patternTester != null) {
                visualiser.use_numbers = true;
            }
            visualiser.visualise(this.redStates);
            return "State Machine visualised";
        } else {
            String res = "";
            for (State state : this.redStates) {
                res += state.hashCode() + " [" + state.count + "]\n";
                for (Symbol symbol : state.getTransitions()) {
                    State next = state.getState(symbol);
                    res += " " + symbol + " > " + next.hashCode() + " [" + next.count + "]\n";
                }
                res += "\n";
            }
            return res;
        }
    }

}