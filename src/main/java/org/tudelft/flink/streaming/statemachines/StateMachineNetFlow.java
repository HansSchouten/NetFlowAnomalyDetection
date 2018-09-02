package org.tudelft.flink.streaming.statemachines;

import com.fasterxml.jackson.databind.JsonNode;
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
     * Show a visualisation of the learned State Machine.
     */
    public static boolean SHOW_VISUALISATION = false;
    /**
     * Show a visualisation for each step of learning the State Machine.
     */
    public static boolean VISUALISE_STEPS = false;
    /**
     * Output a file containing the visualised State Machine.
     */
    public static boolean OUTPUT_VISUALISATION_FILE = true;
    /**
     * Turn on/off depending on whether we are running on NetFlow or PAutomaC sequences (with stop symbols).
     */
    public static boolean PAUTOMAC_TEST = false;
    /**
     * Output a file containing all encountered patterns (for debugging purposes).
     */
    public static boolean OUTPUT_PATTERN_FILE = false;
    //public PatternFileOutput patternFileOutput = new PatternFileOutput(FUTURE_SIZE);

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

    public char current_char = 'A';
    /**
     * The NetFlows inside this NetFlow.
     */
    public List<StateMachineNetFlow> dataset;
    /**
     * The number of red states at which the last visualisation is made.
     */
    public int redStateVisualiseCount = 1;
    /**
     * Object for matching this State Machine with known fingerprints.
     */
    public FingerprintMatcher fingerprintMatcher;

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
        this.dataset = new ArrayList<>();

        JsonNode jsonDataset = super.getFromString(line);
        if (jsonDataset == null) {
            return;
        }

        for (JsonNode parameters : jsonDataset) {
            StateMachineNetFlow flow = new StateMachineNetFlow();
            flow.setFromJsonNode(parameters);
            flow.setSingleFlow();
            // symbolconfig is for testing purposes
            flow.symbolConfig = new SymbolConfig();

            /*
            // Eduroam skip massive StateMachine
            if (flow.srcIP.equals("177.32.252.28") && flow.dstIP.equals("176.71.103.118")) {
                continue;
            }
            if (flow.srcIP.equals("176.71.103.118") && flow.dstIP.equals("177.32.252.28")) {
                continue;
            }
            */

            this.dataset.add(flow);
        }
    }

    public void setSingleFlow() {
        this.stateMachineID = this.srcIP + "-" + this.dstIP + "-" + this.protocol.toString();
    }

    public void appendFile(String line) {
        try
        {
            String filename= "smb.log";
            FileWriter fw = new FileWriter(filename,true);
            fw.write(line + "\n");
            fw.close();
        }
        catch(IOException ioe)
        {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

    public long count = 0;
    public void consumeNetFlow(StateMachineNetFlow nextNetFlow) {
        if (nextNetFlow.dstPort.equals("445")) {
            String newLine = nextNetFlow.toString();
            appendFile(newLine);
        }

        if (this.count % 1000000 == 0 || this.count == 0) {
            System.out.println("Number of flows processed: " + this.count);
        }
        this.count++;

        if (true) {
            return;
        }







        if (this.redStates.size() > 14) {
            resetStateMachine();
        }

        // start with processing this object, before processing all rolling NetFlows
        if (this.flow_counter == 0) {
            //System.out.println(this.stateMachineID + " reducing first NetFlow");
            long now = System.nanoTime();
            log(String.valueOf(now));

            this.future = new LinkedList<>();
            this.root = new State(State.Color.RED, 0);
            this.root.setLabel(String.valueOf(this.current_char));
            nextChar();

            this.instances = new HashMap<>();
            this.instances.put(this.root.hashCode(), this.root);

            this.redStates = new LinkedList<>();
            this.redStates.add(this.root);

            log(Integer.toString(this.root.hashCode()));
            log("=====");
            processFlow(this);
            //addForPercentile(this);
        }

        if (this.redStates.size() > redStateVisualiseCount && this.redStates.size() > 6) {
            this.toString();
            this.redStateVisualiseCount++;
        }

        // process incoming NetFlow
        processFlow(nextNetFlow);
        //addForPercentile(nextNetFlow);
    }

    public void resetStateMachine() {
        this.current_char = 'A';
        this.flow_counter = 0;
        this.skip_counter = 0;
        this.redStateVisualiseCount = 1;
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

        if (PAUTOMAC_TEST) {
            // if we encounter a reset symbol, reset all instances      // DISABLE THIS BLOCK ON NETFLOW STREAMS
            Symbol resetSymbol = new Symbol("-1");
            for (Symbol futureSymbol : this.future) {
                if (futureSymbol.equals(resetSymbol) || this.currentSymbol.equals(resetSymbol)) {
                    this.skip_counter++;
                    this.instances = new HashMap<>();
                    this.instances.put(this.root.hashCode(), this.root);
                    return;
                }
            }
        } else {
            // all futures start in the root, so add the root to the list of states to evaluate the current future on
            this.instances.put(this.root.hashCode(), this.root);      // ENABLE THIS LINE ON NETFLOW STREAMS
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
        Map<Integer, State> newInstances = new HashMap<>();

        for (Integer instanceKey : this.instances.keySet()) {
            // get instance state
            State instance = this.instances.get(instanceKey);

            // increase occurrence frequency of this future in this state
            instance.increaseFrequency(this.future);

            // merge or color the state if a significant amount of futures are captured in this state
            if (instance.getColor() == State.Color.BLUE && instance.isSignificant()) {
                log("Significant State: " + instance.hashCode());

                State mostSimilarState = instance.getMostSimilarState(this.redStates);
                if (mostSimilarState == null) {
                    // color state red if it could not be merged with any other red state
                    instance.changeToRed();
                    this.redStates.add(instance);
                } else {
                    // merge instance with red state (pointing all incoming transitions to the red state instead)
                    instance.merge(mostSimilarState);
                    // continue evaluation in the red state the instance has merged with
                    instance = mostSimilarState;
                }

                if (VISUALISE_STEPS) {
                    BlueFringeVisualiser visualiser = new BlueFringeVisualiser(false);
                    visualiser.visualise(this.redStates);
                    visualiser.showVisualisation();
                    visualiser.writeToFile(this.stateMachineID);
                }
            }

            // get the next state in the direction of the symbol of the consumed NetFlow
            State next = instance.getState(this.currentSymbol, this.current_char);
            if (next != null && next.new_state) {
                if (VISUALISE_STEPS) {
                    BlueFringeVisualiser visualiser = new BlueFringeVisualiser(false);
                    visualiser.visualise(this.redStates);
                    visualiser.showVisualisation();
                    visualiser.writeToFile(this.stateMachineID);
                }
                next.new_state = false;

                nextChar();
            }

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
        if (true) {
            return this.srcIP
                + "," + this.srcPort
                + "," + this.protocol
                + "," + this.dstIP
                + "," + this.dstPort
                + "," + this.byteCount
                + "," + this.packetCount
                + "," + (int) this.averagePacketSize;
        }

        // match with malware fingerprints
        this.matchMalware(this.stateMachineID);

        if (true) {
            return "";
        }

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
            //this.patternFileOutput.writeToFile(this.stateMachineID);
        }

        //printPercentiles();

        return "";
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
     * Match this State Machine with malware fingerprints.
     *
     * @param stateMachineID
     */
    public void matchMalware(String stateMachineID) {
        if (this.redStates.size() <= 2) {
            return;
        }

        if (this.fingerprintMatcher == null) {
            this.fingerprintMatcher = new FingerprintMatcher();
            this.fingerprintMatcher.loadFingerprints();
        }

        this.fingerprintMatcher.match(this.root, stateMachineID);
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
        String timeStamp = new SimpleDateFormat("HHmmss").format(Calendar.getInstance().getTime());
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
        int index = (int) Math.floor(this.packetSizes.size() / 3);
        System.out.println("Packet Sizes");
        System.out.println(this.packetSizes.get(0));
        System.out.println("33th percentile: " + this.packetSizes.get(index));
        index = (int) Math.floor(this.packetSizes.size() / 3 * 2);
        System.out.println("66th percentile: " + this.packetSizes.get(index));
        System.out.println(this.packetSizes.get(this.packetSizes.size()-1));
        System.out.println();

        Collections.sort(this.packetCounts);
        index = (int) Math.floor(this.packetCounts.size() / 3);
        System.out.println("Number of packets per flow");
        System.out.println(this.packetCounts.get(0));
        System.out.println("33th percentile: " + this.packetCounts.get(index));
        index = (int) Math.floor(this.packetCounts.size() / 3 * 2);
        System.out.println("66th percentile: " + this.packetCounts.get(index));
        System.out.println(this.packetCounts.get(this.packetCounts.size()-1));
        System.out.println();
    }

}