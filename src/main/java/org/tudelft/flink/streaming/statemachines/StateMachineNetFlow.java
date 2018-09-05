package org.tudelft.flink.streaming.statemachines;

import com.fasterxml.jackson.databind.JsonNode;
import javafx.util.Pair;
import org.tudelft.flink.streaming.NetFlow;
import org.tudelft.flink.streaming.statemachines.visualisation.BlueFringeVisualiser;
import org.tudelft.flink.streaming.statemachines.helpers.SymbolConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class StateMachineNetFlow extends NetFlow {

    /**
     * Number of symbols contained in a future.
     */
    public static int FUTURE_SIZE = 4;
    /**
     * Maximum number of instances past by one sequence of futures (prevents infinite loops).
     */
    public static int MAX_INSTANCE_DEPTH = 10;


    /**
     * Different execution modes.
     */
    public enum Mode {
        PAUTOMAC_VALIDATION,        // process PAutomaC sequences, containing stop symbols.
        LEARN_ATTACK_MODELS,
        REALTIME_DETECTION
    }
    /**
     * The current execution mode.
     */
    public Mode mode = Mode.REALTIME_DETECTION;
    /**
     * Show a visualisation for each step of learning the State Machine.
     */
    public static boolean VISUALISE_STEPS = false;
    /**
     * Output a file containing all encountered patterns (for debugging purposes).
     */
    public static boolean OUTPUT_PATTERN_FILE = false;


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
    public Map<Integer, Pair<Integer, State>> instances;
    /**
     * The list of all red states (states that are part of the final State Machine).
     */
    public List<State> redStates = new LinkedList<>();
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
     * Label of the next state that will be added.
     */
    public char current_char = 'A';
    /**
     * The number of red states at which the last visualisation is made.
     */
    public int previousRedStateCount = 1;
    /**
     * Object for matching this State Machine with known fingerprints.
     */
    public FingerprintMatcher fingerprintMatcher;
    /**
     * Object for outputting the observed NetFlow sequences.
     */
    //public PatternFileOutput patternFileOutput = new PatternFileOutput(FUTURE_SIZE);
    /**
     * The NetFlows inside this NetFlow.
     */
    public List<StateMachineNetFlow> dataset;

    public boolean completed = false;


    /**
     * Set NetFlow parameters from the given input string.
     *
     * @param line
     */
    @Override
    public void setFromString(String line) {
        this.dataset = new ArrayList<>();

        JsonNode jsonDataset = super.getFromString(line);
        if (jsonDataset == null) {
            return;
        }

        for (JsonNode parameters : jsonDataset) {
            StateMachineNetFlow flow = new StateMachineNetFlow();
            flow.setFromJsonNode(parameters);
            flow.setSingleFlow();
            this.dataset.add(flow);
        }
    }

    public void setSingleFlow() {
        this.stateMachineID = this.srcIP + "-" + this.dstIP + "-" + this.protocol.toString();// + "-day" + this.start;
    }

    /**
     * Consume an incoming NetFlow.
     *
     * @param nextNetFlow
     */
    public void consumeNetFlow(StateMachineNetFlow nextNetFlow) {
        if (nextNetFlow.protocol != Protocol.TCP || this.completed) { // || this.dstPort != "443") {
            this.completed = true;
            return;
        }

        // start with processing this object, before processing all rolling NetFlows
        if (this.flow_counter == 0) {
            //System.out.println(this.stateMachineID + " reducing first NetFlow");
            resetStateMachine();
            processFlow(this);
        }

        processFlow(nextNetFlow);
        performModeSpecificAction();
    }

    /**
     * Reset all parameters of this State Machine.
     */
    public void resetStateMachine() {
        this.current_char = 'A';
        this.flow_counter = 0;
        this.skip_counter = 0;
        this.previousRedStateCount = 1;

        this.future = new LinkedList<>();
        this.root = new State(State.Color.RED, 0);
        this.root.setLabel(String.valueOf(this.current_char));
        nextChar();

        this.instances = new HashMap<>();
        this.instances.put(this.root.hashCode(), new Pair(0, this.root));

        this.redStates = new LinkedList<>();
        this.redStates.add(this.root);

        // symbol config should be published across nodes
        this.symbolConfig = new SymbolConfig();
    }

    /**
     * Perform an action corresponding to the current mode, if the model has changed.
     */
    public void performModeSpecificAction() {
        if (this.redStates.size() >= 1 && this.redStates.size() > previousRedStateCount) {
            this.previousRedStateCount++;

            if (this.mode == Mode.LEARN_ATTACK_MODELS) {
                visualiseMalwareModel();
            } else if (this.mode == Mode.REALTIME_DETECTION) {
                matchMalware();
                if (this.redStates.size() >= 20) {
                    resetStateMachine();
                    this.completed = true;
                }
            } else if(this.mode == Mode.PAUTOMAC_VALIDATION) {
            }
        }
    }

    /**
     * Visualise the current progress of learning a malware model.
     */
    public void visualiseMatchingMalwareModel() {
        BlueFringeVisualiser visualiser = new BlueFringeVisualiser(true);
        visualiser.visualise(this.redStates);
        visualiser.writeToFile(this.stateMachineID);
    }

    /**
     * Visualise the current progress of learning a malware model.
     */
    public int increasing_int = 0;
    public void visualiseMalwareModel() {
        BlueFringeVisualiser visualiser = new BlueFringeVisualiser(true);
        String tmp = this.stateMachineID;
        this.stateMachineID += "-" + this.increasing_int;
        this.increasing_int++;
        visualiser.visualise(this.redStates);
        visualiser.writeToFile(this.stateMachineID);
        if (this.mode == Mode.LEARN_ATTACK_MODELS) {
            outputRandomTraces();
        }
        this.stateMachineID = tmp;
    }

    /**
     * Match this State Machine with malware fingerprints.
     */
    public void matchMalware() {
        // initialise fingerprint matcher
        if (this.fingerprintMatcher == null) {
            this.fingerprintMatcher = new FingerprintMatcher();
            this.fingerprintMatcher.loadFingerprints();
        }

        this.fingerprintMatcher.match(this);
    }

    /**
     * Process the given NetFlow and update rolling state parameters.
     *
     * @param nextNetFlow
     */
    public void processFlow(StateMachineNetFlow nextNetFlow) {
        // increase the counter, keeping track of how many flows are reduced into this object
        this.flow_counter++;
        //addForPercentile(nextNetFlow);

        // get the symbol of the incoming NetFlow
        Symbol incomingSymbol = getSymbol(nextNetFlow);

        // update the future
        if (this.future.size() == FUTURE_SIZE) {
            this.currentSymbol = this.future.peek();
        } else {
            this.future.add(incomingSymbol);
            // only start processing the future if it has a sufficient size
            return;
        }

        // add future to the collection of all encountered patterns (only used for debugging)
        if (OUTPUT_PATTERN_FILE) {
            //patternFileOutput.addPattern(this.future);
        }

        if (this.mode == Mode.PAUTOMAC_VALIDATION) {
            // if we encounter a reset symbol, reset all instances
            Symbol resetSymbol = new Symbol("-1");
            for (Symbol futureSymbol : this.future) {
                if (this.currentSymbol.equals(resetSymbol) || futureSymbol.equals(resetSymbol)) {
                    this.skip_counter++;
                    this.instances = new HashMap<>();
                    this.instances.put(this.root.hashCode(), new Pair(0, this.root));
                    // update future for next iteration
                    this.future.poll();
                    this.future.add(incomingSymbol);
                    // skip to next iteration
                    return;
                }
            }
        } else {
            // all futures start in the root, so add the root to the list of states to evaluate the current future on
            this.instances.put(this.root.hashCode(), new Pair(0, this.root));
        }

        // evaluate this future in all state instances (increasing the occurrence frequency of this future)
        evaluateInstances();

        // update future for next iteration
        this.future.poll();
        this.future.add(incomingSymbol);
    }

    /**
     * Increase the occurrence frequency of the current future in the instance states and move each instance to the next state.
     */
    protected void evaluateInstances() {
        Map<Integer, Pair<Integer, State>> newInstances = new HashMap<>();

        for (Integer instanceKey : this.instances.keySet()) {
            // get instance state
            Pair<Integer, State> pair = this.instances.get(instanceKey);
            State instance = pair.getValue();
            Integer instanceDepth = pair.getKey();

            // increase occurrence frequency of this future in this state
            instance.increaseFrequency(this.future);

            // merge or color the state if a significant amount of futures are captured in this state
            if (instance.getColor() == State.Color.BLUE && instance.isSignificant()) {
                // change white child states into blue states
                instance.colorChildsBlue();

                State mostSimilarState = instance.getMostSimilarState(this.redStates);
                if (mostSimilarState == null) {
                    log("New red state");
                    // color state red if it could not be merged with any other red state
                    instance.changeToRed();
                    this.redStates.add(instance);
                } else {
                    log("Merge with red state, " + this.redStates.size() + " red states");
                    // merge instance with red state (pointing all incoming transitions to the red state instead)
                    instance.merge(mostSimilarState);
                    // continue evaluation in the red state the instance has merged with
                    instance = mostSimilarState;
                }

                visualiseStep();
            }

            // get the next state in the direction of the symbol of the consumed NetFlow
            State next = instance.getState(this.currentSymbol, this.current_char);
            if (next != null && next.new_state) {
                visualiseStep();
                next.new_state = false;
                nextChar();
            }

            // if a state in the direction of the current symbol exists, add the next state to instances
            if (next != null && instanceDepth < MAX_INSTANCE_DEPTH) {
                newInstances.put(next.hashCode(), new Pair(instanceDepth + 1, next));
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

    protected void visualiseStep() {
        if (VISUALISE_STEPS) {
            BlueFringeVisualiser visualiser = new BlueFringeVisualiser(false);
            visualiser.visualise(this.redStates);
            //visualiser.showVisualisation();
            visualiser.writeToFile(this.stateMachineID);

            System.out.println("flows:" + this.flow_counter);
            System.out.println("skipped: " + this.skip_counter);
        }
    }

    /**
     * Return the string representation of this rolling StateMachineNetFlow.
     *
     * @return
     */
    @Override
    public String toString() {
        /*
        log(this.stateMachineID + " has " + this.flow_counter + " flows");
        log("#skipped sequences due to stop symbol: " + Integer.toString(this.skip_counter));
        long now = System.nanoTime();
        log(String.valueOf(now));

        // output random traces
        this.outputRandomTraces();

        if (SHOW_VISUALISATION || OUTPUT_VISUALISATION_FILE) {
            log("VISUALISING");
            BlueFringeVisualiser visualiser = new BlueFringeVisualiser(true);
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

        //printPercentiles();
        */

        return this.srcIP
                + "," + this.srcPort
                + "," + this.protocol
                + "," + this.dstIP
                + "," + this.dstPort
                + "," + this.byteCount
                + "," + this.packetCount
                + "," + (int) this.averagePacketSize;
    }

    /**
     * Update the current char to the next character of the alphabet.
     *
     * @return
     */
    public char nextChar() {
        int value = this.current_char;
        this.current_char = Character.valueOf((char) (value + 1));
        return this.current_char;
    }

    /**
     * Generate random traces and write them to file.
     */
    public void outputRandomTraces() {
        HashMap<String, Double> traces = new HashMap<>();

        for (int i = 0; i < 10000; i++) {
            State state = this.root;

            Symbol transition = state.getRandomTransition();
            double chance = state.getTransitionProbability(transition);
            String sequence = transition.toString();

            for (int s = 1; s < 3; s++) {
                state = state.getState(transition, null);
                if (state == null) {
                    sequence = null;
                    break;
                }
                transition = state.getRandomTransition();
                if (transition == null) {
                    sequence = null;
                    break;
                }
                chance *= state.getTransitionProbability(transition);
                sequence += " " + transition.toString();
            }

            if (sequence != null) {
                traces.put(sequence, chance);
            }
        }

        for (String trace : traces.keySet()) {
            System.out.println(trace + " : " + traces.get(trace));
        }

        String cleanID = this.stateMachineID.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
        String timeStamp = new SimpleDateFormat("HHmmss.SSS").format(Calendar.getInstance().getTime());
        String path = "output\\state-machines\\traces-" + cleanID + "-" + timeStamp + ".txt";
        System.out.println(path);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            // write each trace
            for (String trace : traces.keySet()) {
                writer.write(3 + " " + trace + " " + traces.get(trace) + "\n");
            }
            writer.close();
        }
        catch(IOException ex) {
            System.out.println("Error writing State Machine trace file:\n" + ex.getMessage());
        }
    }

    /**
     * Write log messages to console.
     *
     * @param message
     */
    public void log(String message) {
        //System.out.println(message);
    }






    public List<Double> packetSizes;
    public List<Long> packetCounts;
    public void addForPercentile(StateMachineNetFlow other) {
        if (! other.protocol.equals(Protocol.TCP)) {
            return;
        }

        if (this.packetCounts == null) {
            this.packetCounts = new ArrayList<>();
            this.packetSizes = new ArrayList<>();
        }

        this.packetSizes.add(other.averagePacketSize);
        this.packetCounts.add(other.packetCount);
    }

    public void printPercentiles() {
        System.out.println();


        Collections.sort(this.packetSizes);
        int index = (int) Math.floor(this.packetSizes.size() / 4);
        System.out.println("Packet Sizes");
        System.out.println(this.packetSizes.get(0));

        System.out.println("25th percentile: " + this.packetSizes.get(index));
        index = (int) Math.floor(this.packetSizes.size() / 4 * 2);

        System.out.println("50th percentile: " + this.packetSizes.get(index));
        index = (int) Math.floor(this.packetSizes.size() / 4 * 3);

        System.out.println("75th percentile: " + this.packetSizes.get(index));
        System.out.println(this.packetSizes.get(this.packetSizes.size()-1));
        System.out.println();


        Collections.sort(this.packetCounts);
        index = (int) Math.floor(this.packetCounts.size() / 4);
        System.out.println("Number of packets per flow");
        System.out.println(this.packetCounts.get(0));

        System.out.println("25th percentile: " + this.packetCounts.get(index));
        index = (int) Math.floor(this.packetCounts.size() / 4 * 2);

        System.out.println("50th percentile: " + this.packetCounts.get(index));
        index = (int) Math.floor(this.packetCounts.size() / 4 * 3);

        System.out.println("75th percentile: " + this.packetCounts.get(index));
        System.out.println(this.packetCounts.get(this.packetCounts.size()-1));
        System.out.println();
    }

    public void appendFile(String line) {
        try {
            String filename= "output\\" + this.stateMachineID + ".txt";
            FileWriter fw = new FileWriter(filename,true);
            fw.write(line + "\n");
            fw.close();
        } catch(IOException ex) {
            System.err.println("IOException: " + ex.getMessage());
        }
    }

}