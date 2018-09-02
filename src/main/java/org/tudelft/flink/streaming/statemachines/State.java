package org.tudelft.flink.streaming.statemachines;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.tudelft.flink.streaming.RandomWeightedCollection;

import java.nio.ByteBuffer;
import java.util.*;

public class State {

    /**
     * the number of futures processed by a state at which the state is merged or becomes a red state.
     */
    public final int SIGNIFICANCE_BOUNDARY = 100;
    /**
     * the upper bound of the Chi-distance below which the sketches are regarded as similar.
     */
    protected double CHI_SIMILARITY = 0.5;
    /**
     * the lower bound of the cosine similarity, above which the sketches are regarded as similar.
     */
    protected double COSINE_SIMILARITY = 0.90;
    protected int DEPTH = 10;
    protected int WIDTH = 100;
    public boolean new_state = false;

    public enum Color {
        RED,
        BLUE
    }

    protected Map<Symbol, State> transitions;
    protected Map<Symbol, Integer> transitionCounts;
    protected Map<Symbol, State> inLinks;
    protected CountMinSketch sketch;
    protected Color color;
    protected int count;
    protected int depth;
    protected String label;
    protected RandomWeightedCollection randomTransitions;

    /**
     * State constructor.
     *
     * @param color
     */
    public State(Color color, int depth) {
        this.transitions = new HashMap<>();
        this.transitionCounts = new HashMap<>();
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

    public void setLabel(String label) {
        //this.label = label;
    }

    public String getLabel() {
        return this.label;
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

        // increase number of sequences through this state
        this.count++;

        // increase transition count of the symbol that is the head of the queue
        Symbol transitionSymbol = future.peek();
        int count = 1;
        if (this.transitionCounts.containsKey(transitionSymbol)) {
            count = this.transitionCounts.get(transitionSymbol) + 1;
        }
        this.transitionCounts.put(transitionSymbol, count);
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
     * Return whether this state has a transition with the given symbol.
     *
     * @param transition
     * @return
     */
    public boolean hasTransition(Symbol transition) {
        return this.transitions.containsKey(transition);
    }

    /**
     * Return the state in the direction of the given transition symbol.
     *
     * @param transition
     * @param current_char
     * @return
     */
    public State getState(Symbol transition, Character current_char) {
        if (this.transitions.containsKey(transition)) {
            return this.transitions.get(transition);
        } else {
            // if no transition exists yet, but the state is part of the final State Machine, create a new blue state
            if (this.color == Color.RED && this.depth < Math.max(StateMachineNetFlow.FUTURE_SIZE, 8)) {
                State newState = new State(Color.BLUE, this.depth + 1);
                if (current_char != null) {
                    newState.setLabel(String.valueOf(current_char));
                }
                setTransition(transition, newState);
                newState.addInLink(transition,this);
                newState.new_state = true;
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
     * Return the state which has a sketch that is most similar to this state's sketch.
     * Returns null however, if no state similarity passes the threshold.
     *
     * @param otherStates
     * @return
     */
    public State getMostSimilarState(List<State> otherStates) {
        double most_similar = 0;
        State mostSimilarState = null;
        // find most similar state
        for (State other : otherStates) {
            double similarity = getSimilarity(other);
            if (similarity > most_similar) {
                mostSimilarState = other;
                most_similar = similarity;
            }
        }
        // return most similar state if it exceeds the boundary
        if (most_similar > COSINE_SIMILARITY) {
            return mostSimilarState;
        }
        return null;
    }

    /**
     * Return whether the distribution of futures passed through both states are similar enough to merge.
     *
     * @param other
     * @return
     */
    public boolean similarTo(State other) {
        return getSimilarity(other) > COSINE_SIMILARITY;
    }

    /**
     * Return the similarity of this state's sketch compared to the given other state's sketch.
     *
     * @param other
     * @return
     */
    public double getSimilarity(State other) {
        long[] sketch1 = this.getSketchVector();
        long[] sketch2 = other.getSketchVector();

        /*
        System.out.println(this.hashCode() + ":");
        for (int i = 0; i < sketch1.length; i++) {
            System.out.print(sketch1[i] + ",");
        }
        System.out.println("");
        System.out.println(other.hashCode() + ":");
        for (int i = 0; i < sketch2.length; i++) {
            System.out.print(sketch2[i] + ",");
        }
        */

        double similarity = cosineSimilarity(sketch1, sketch2);
        //System.out.println("");
        //System.out.println("SIM: " + similarity);
        return similarity;
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

    /**
     * Return the cosine similarity between the given vectors.
     *
     * @param v1
     * @param v2
     * @return
     */
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

    /**
     * Normalize the given vector.
     *
     * @param vector
     * @return
     */
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

    /**
     * Return a random transition symbol (all possible symbols have equal chance).
     *
     * @return
     */
    public Symbol getRandomTransition() {
        // re-initialize, since transitions could have been added
        this.randomTransitions = new RandomWeightedCollection();
        int count = 0;
        for (Symbol symbol : this.transitionCounts.keySet()) {
            State next = getState(symbol, null);
            if (next.getColor().equals(Color.BLUE)) {
                continue;
            }
            count++;
            double chance = 1.0 / (double) this.transitionCounts.keySet().size();
            this.randomTransitions.add(chance, symbol);
        }

        // return a random Symbol
        if (count > 0) {
            return (Symbol) this.randomTransitions.randomEntry();
        }
        return null;
    }

    /**
     * Return the occurrence probability of the given symbol.
     *
     * @param symbol
     * @return
     */
    public Double getTransitionProbability(Symbol symbol) {
        if (this.transitionCounts.containsKey(symbol)) {
            return this.transitionCounts.get(symbol) / (double) this.count;
        } else {
            return null;
        }
    }

}
