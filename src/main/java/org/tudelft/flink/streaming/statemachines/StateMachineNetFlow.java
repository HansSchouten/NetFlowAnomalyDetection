package org.tudelft.flink.streaming.statemachines;

import org.tudelft.flink.streaming.NetFlow;
import org.tudelft.flink.streaming.statemachines.helpers.PatternFileOutput;
import org.tudelft.flink.streaming.statemachines.visualisation.BlueFringeVisualiser;
import org.tudelft.flink.streaming.statemachines.helpers.SymbolConfig;

import java.util.*;

public class StateMachineNetFlow extends NetFlow {

    /**
     * Number of symbols contained in a future.
     */
    public static int FUTURE_SIZE = 3;
    /**
     * Show a visualisation of the learned State Machine.
     */
    public static boolean SHOW_VISUALISATION = true;
    /**
     * Output a file containing the visualised State Machine.
     */
    public static boolean OUTPUT_VISUALISATION_FILE = true;
    /**
     * Output a file containing all encountered patterns (for debugging purposes).
     */
    public static boolean OUTPUT_PATTERN_FILE = false;
    public PatternFileOutput patternFileOutput = new PatternFileOutput(FUTURE_SIZE);

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
     * The symbol configuration that is used to map NetFlows to symbols.
     */
    public SymbolConfig symbolConfig;
    /**
     * The name/identifier used when mentioning or saving data regarding this State Machine.
     */
    public String stateMachineID;
    /**
     * Count how many flows are reduced into this StateMachineNetFlow.
     */
    public int flow_counter = 0;
    /**
     * Number of skipped futures, due to encountering stop symbols.
     */
    public int skip_counter = 0;

    /**
     * StateMachineNetFlow constructor.
     */
    public StateMachineNetFlow() {
    }

    /**
     * Set NetFlow parameters from the given input string.
     *
     * @param line
     */
    @Override
    public void setFromString(String line) {
        super.setFromString(line);
        this.stateMachineID = this.srcIP + "-" + this.dstIP;
    }

    public void consumeNetFlow(StateMachineNetFlow nextNetFlow) {
        // start with processing this object, before processing all rolling NetFlows
        if (this.flow_counter == 0) {
            log(this.stateMachineID + " reducing first NetFlow");

            this.root = new State(State.Color.RED, 0);
            this.future = new LinkedList<>();

            this.instances = new HashMap<>();
            this.instances.put(this.root.hashCode(), this.root);

            this.redStates = new LinkedList<>();
            this.redStates.add(this.root);

            log(Integer.toString(this.root.hashCode()));
            log("=====");
            processFlow(this);
        }
        // process incoming NetFlow
        processFlow(nextNetFlow);
    }

    /**
     * Consume a new NetFlow and update rolling state parameters.
     *
     * @param nextNetFlow
     */
    public void processFlow(StateMachineNetFlow nextNetFlow) {
        // increase the counter, keeping track of how many flows are reduced into this object
        this.flow_counter++;

        // get the symbol of the incoming NetFlow
        Symbol incomingSymbol = getSymbol(nextNetFlow);

        // update the future
        if (this.future.size() == FUTURE_SIZE) {
            this.currentSymbol = this.future.poll();
        }
        this.future.add(incomingSymbol);

        // only start processing the future if it has a sufficient size
        if (this.currentSymbol == null) {
            return;
        }

        // add future to the collection of all encountered patterns (only used for debugging)
        if (OUTPUT_PATTERN_FILE) {
            patternFileOutput.addPattern(this.future);
        }

        // all futures start in the root, so add the root to the list of states to evaluate the current future on
        //this.instances.put(this.root.hashCode(), this.root);      // ADD THIS LINE ON NETFLOW STREAMS

        // DEBUG
        /*
        if (this.currentSymbol != null) {
            this.log("Current: " + this.currentSymbol.toString());
        }
        for (Symbol s : this.future) {
            this.log(s.toString());
        }
        */

        // if we encounter a reset symbol, reset all instances      // REMOVE THIS BLOCK ON NETFLOW STREAMS
        Symbol resetSymbol = new Symbol("-1");
        for (Symbol futureSymbol : this.future) {
            if (futureSymbol.equals(resetSymbol) || this.currentSymbol.equals(resetSymbol)) {
                this.skip_counter++;
                this.instances = new HashMap<>();
                this.instances.put(this.root.hashCode(), this.root);
                return;
            }
        }
        //this.log("----");

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
            if (instance.getColor() == State.Color.BLUE && instance.isSignificant()) {
                System.out.println("Significant State: " + instance.hashCode());
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
        if (this.symbol != null) {
            return new Symbol(netFlow.symbol);
        }

        return this.symbolConfig.getSymbol(netFlow);
    }

    /**
     * Return the string representation of this rolling StateMachineNetFlow.
     *
     * @return
     */
    @Override
    public String toString() {
        if (SHOW_VISUALISATION || OUTPUT_VISUALISATION_FILE) {
            log(this.stateMachineID + " has " + this.flow_counter + " flows");
            log("#skipped sequences due to stop symbol: " + Integer.toString(this.skip_counter));

            BlueFringeVisualiser visualiser = new BlueFringeVisualiser(false);
            visualiser.visualise(this.redStates);

            if (SHOW_VISUALISATION) {
                visualiser.showVisualisation();
            }
            if (OUTPUT_VISUALISATION_FILE) {
                visualiser.writeToFile(this.stateMachineID);
            }
        }

        if (OUTPUT_PATTERN_FILE) {
            this.patternFileOutput.writeToFile(this.stateMachineID);
        }

        return "";
    }

    public void log(String message) {
        System.out.println(message);
    }

}