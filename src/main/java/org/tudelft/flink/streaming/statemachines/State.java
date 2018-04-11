package org.tudelft.flink.streaming.statemachines;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.nio.ByteBuffer;
import java.util.*;

public class State {

    /**
     * the number of futures processed by a state at which the state is merged or becomes a red state.
     */
    public final int SIGNIFICANCE_BOUNDARY = 200;
    /**
     * the upper bound of the Chi-distance below which the sketches are regarded as similar.
     */
    protected double CHI_SIMILARITY = 0.5;
    protected int DEPTH = 10;
    protected int WIDTH = 100;

    public enum Color {
        RED,
        BLUE
    }

    protected Map<Symbol, State> transitions;
    protected Map<Symbol, State> inLinks;
    protected CountMinSketch sketch;
    protected Color color;
    protected int count;
    protected int depth;

    /**
     * State constructor.
     *
     * @param color
     */
    public State(Color color, int depth) {
        this.transitions = new HashMap<>();
        this.inLinks = new HashMap<>();
        this.sketch = new CountMinSketch(DEPTH, WIDTH, 1);
        this.color = color;
        this.count = 0;
        this.depth = depth;
    }

    /**
     * Increase the occurrence frequency of this future in this state's sketch.
     *
     * @param future
     */
    public void increaseFrequency(Queue<Symbol> future)
    {
        // since we compare without normalization, allow at most SIGNIFICANCE_BOUNDARY patterns in the sketch
        if (this.count < SIGNIFICANCE_BOUNDARY) {
            String signature = futureToString(future);
            // increase the occurrence frequency of the future by one
            this.sketch.add(signature, 1);
        }
        this.count++;
    }

    /**
     * Get the string representation of the series of transitions describing this future.
     *
     * @param future
     * @return
     */
    protected String futureToString(Queue<Symbol> future) {
        String res = "";
        for (Symbol transition : future) {
            res += transition.toString() + "-";
        }
        return res;
    }

    /**
     * Return all possible transitions.
     *
     * @return
     */
    public Set<Symbol> getTransitions() {
        return this.transitions.keySet();
    }

    /**
     * Add a link from the given origin to this state via the given symbol.
     *
     * @param symbol
     * @param origin
     */
    public void addInLink(Symbol symbol, State origin) {
        this.inLinks.put(symbol, origin);
    }

    /**
     * Set the transition with the given symbol to the given next state.
     *
     * @param symbol
     * @param next
     */
    public void setTransition(Symbol symbol, State next) {
        this.transitions.put(symbol, next);
    }

    /**
     * Return the state in the direction of the given transition symbol.
     *
     * @param transition
     * @return
     */
    public State getState(Symbol transition) {
        if (this.transitions.containsKey(transition)) {
            return this.transitions.get(transition);
        } else {
            // if no transition exists yet, but the state is part of the final State Machine, create a new blue state
            if (this.color == Color.RED && this.depth < StateMachineNetFlow.FUTURE_SIZE) {
                State newState = new State(Color.BLUE, this.depth + 1);
                setTransition(transition, newState);
                newState.addInLink(transition,this);
                return newState;
            }
        }
        return null;
    }

    /**
     * Return whether the number of futures passed through this state is statistical significant for comparing state similarities.
     *
     * @return
     */
    public boolean isSignificant() {
        return (this.count == SIGNIFICANCE_BOUNDARY);
    }

    /**
     * Change the color of this state to red.
     */
    public void changeToRed() {
        this.color = Color.RED;
    }

    /**
     * Merge this state with the passed red by diverting all incoming links to the red state.
     *
     * @param redState
     */
    public void merge(State redState) {
        for (Symbol inLinkSymbol : this.inLinks.keySet()) {
            State origin = this.inLinks.get(inLinkSymbol);
            origin.setTransition(inLinkSymbol, redState);
        }
    }

    /**
     * Return whether the distribution of futures passed through both states are similar enough to merge.
     *
     * @param redState
     * @return
     */
    public boolean similarTo(State redState) {
        long[] sketch1 = toLongArray(CountMinSketch.serialize(this.sketch));
        long[] sketch2 = toLongArray(CountMinSketch.serialize(redState.sketch));
        return chiSquare(sketch1, sketch2) < CHI_SIMILARITY;
    }

    /**
     * Convert a byte array to a long array.
     *
     * @param byteArray
     * @return
     */
    public long[] toLongArray(byte[] byteArray) {
        long[] longArray = new long[byteArray.length / 8];
        for (int i = 2; i < byteArray.length / 8; i++) {
            byte[] longBytes = Arrays.copyOfRange(byteArray, i*8, (i + 1)*8);
            ByteBuffer buffer = ByteBuffer.wrap(longBytes);
            longArray[i] = buffer.getLong();
        }
        return longArray;
    }

    /**
     * Gets the Chi Square distance between two arrays.
     *
     * @param sketch1
     * @param sketch2
     * @return The Chi Square distance between both sketches.
     */
    protected double chiSquare(long[] sketch1, long[] sketch2){
        double r = 0;
        for (int i = 0; i < sketch1.length; i++) {
            double t = sketch1[i] + sketch2[i];
            if(t != 0)
                r += Math.pow(sketch1[i] - sketch2[i], 2) / t;
        }
        return 0.5 * r;
    }

}
