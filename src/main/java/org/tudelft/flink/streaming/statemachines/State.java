package org.tudelft.flink.streaming.statemachines;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.nio.ByteBuffer;
import java.util.*;

public class State {

    /**
     * the number of futures processed by a state at which the state is merged or becomes a red state.
     */
    public final int SIGNIFICANCE_BOUNDARY = 500;
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

    public int getCount() {
        return this.count;
    }

    public Color getColor() {
        return this.color;
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
            if (this.color == Color.RED && this.depth < Math.max(StateMachineNetFlow.FUTURE_SIZE, 20)) {
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
     * Return whether this state is the root state.
     *
     * @return
     */
    public boolean isRoot() {
        return this.depth == 0;
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
        long[] sketch1 = this.getSketchVector();
        long[] sketch2 = redState.getSketchVector();

        System.out.println(this.hashCode() + ":");
        for (int i = 0; i < sketch1.length; i++) {
            System.out.print(sketch1[i] + ",");
        }
        System.out.println("");
        System.out.println(redState.hashCode() + ":");
        for (int i = 0; i < sketch2.length; i++) {
            System.out.print(sketch2[i] + ",");
        }
        System.out.println("");
        System.out.println(">" + cosineSimilarity(sketch1, sketch2));
        return cosineSimilarity(sketch1, sketch2) > 0.92;
    }

    public long[] getSketchVector() {
        long[] serialized_long = toLongArray(CountMinSketch.serialize(this.sketch));
        long[] vector = new long[DEPTH * WIDTH];
        // add all values (except the first two and after the first two, each WIDTH-th value since these are the hashes)
        int c = 0;
        for (int i = 2; i < serialized_long.length - 1; i++) {
            if ((i - 2) % (WIDTH + 1) != 0) {
                vector[c] = serialized_long[i];
                c++;
            }
        }
        return vector;
    }

    /**
     * Convert a byte array to a long array.
     *
     * @param byteArray
     * @return
     */
    protected long[] toLongArray(byte[] byteArray) {
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
    protected double chiSquare(long[] sketch1, long[] sketch2) {
        //double[] norm1 = normalize(sketch1);
        //double[] norm2 = normalize(sketch2);

        double r = 0;
        for (int i = 0; i < sketch1.length; i++) {
            r += Math.pow(sketch1[i] - sketch2[i], 2) / sketch2[i];
        }
        return r / sketch1.length;
    }

    protected double cosineSimilarity(long[] v1, long[] v2) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            normA += Math.pow(v1[i], 2);
            normB += Math.pow(v2[i], 2);
        }
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    protected double[] normalize(long[] vector) {
        double max = 0;
        for (int i = 0; i < vector.length; i++) {
            if (vector[i] > max) {
                max = vector[i];
            }
        }
        double[] norm = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            norm[i] = vector[i] / max;
        }
        return norm;
    }

}
