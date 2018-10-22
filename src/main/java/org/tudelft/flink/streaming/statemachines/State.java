package org.tudelft.flink.streaming.statemachines;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.tudelft.flink.streaming.RandomWeightedCollection;

import java.nio.ByteBuffer;
import java.util.*;

public class State {

    /**
     * the number of futures processed by a state at which the state is merged or becomes a red state.
     */
    public int SIGNIFICANCE_BOUNDARY = 100;//10;
    /**
     * the upper bound of the Chi-distance below which the sketches are regarded as similar.
     */
    //protected double CHI_SIMILARITY = 0.5;
    //protected double KL_DIVERGENCE = 0.25;
    /**
     * the lower bound of the cosine similarity, above which the sketches are regarded as similar.
     */
    public double COSINE_SIMILARITY = 0.8;
    public int DEPTH = 5;
    public int WIDTH = 250;
    /**
     * For visualisation purposes.
     */
    public boolean new_state = false;

    public enum Color {
        RED,
        BLUE,
        WHITE
    }

    protected Map<Symbol, State> transitions;
    protected Map<Symbol, Integer> transitionCounts;
    protected Map<Symbol, State> inLinks;
    protected CountMinSketch sketch;
    protected Color color;
    protected int count;
    protected int depth;
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

    public void setCount(int count) {
        this.count = count;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getDepth() {
        return this.depth;
    }

    public boolean hasChildren() {
        return this.transitions.size() > 0;
    }

    public CountMinSketch getSketch() {
        return sketch;
    }

    /**
     * Increase the occurrence frequency of this future in this state's sketch.
     *
     * @param future
     */
    public void increaseFrequency(Queue<Symbol> future) {
        // increase the occurrence frequency of the future by one
        String signature = futureToString(future);
        this.sketch.add(signature, 1);

        // increase the number of sequences through this state
        this.count++;

        // increase transition count of the symbol that is the head of the queue
        Symbol transitionSymbol = future.peek();
        int transitionCount = 1;
        if (this.transitionCounts.containsKey(transitionSymbol)) {
            transitionCount = this.transitionCounts.get(transitionSymbol) + 1;
        }
        this.transitionCounts.put(transitionSymbol, transitionCount);
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
     * Return whether this state is the root state.
     *
     * @return
     */
    public boolean isRoot() {
        return this.depth == 0;
    }

    /**
     * Return whether the number of futures passed through this state is statistical significant for comparing state similarities.
     *
     * @return
     */
    public boolean isSignificant() {
        return (this.count >= SIGNIFICANCE_BOUNDARY);
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
            if (this.depth < Math.max(StateMachineNetFlow.FUTURE_SIZE, 8)) {
                // if no transition exists yet and the maximum depth is not yet reached, create a new blue or white state
                Color newColor = Color.WHITE;
                if (this.color == Color.RED) {
                    newColor = Color.BLUE;
                }

                State newState = new State(newColor, this.depth + 1);
                setTransition(transition, newState);
                newState.addInLink(transition,this);
                newState.new_state = true;
                return newState;
            }
        }
        return null;
    }

    /*
    public void changeSM(int width, int depth) {
        this.WIDTH = width;
        this.DEPTH = depth;
        this.sketch = new CountMinSketch(DEPTH, WIDTH, 1);
    }
    */

    /**
     * Change the color of this state to red.
     */
    public void changeToRed() {
        this.color = Color.RED;
    }

    /**
     * Change the color of this state to blue.
     */
    public void changeToBlue() {
        this.color = Color.BLUE;
    }

    /**
     * Update the color of all (currently white) child states to blue.
     */
    public void colorChildsBlue() {
        for (Symbol symbol : this.transitions.keySet()) {
            State child = getState(symbol);
            child.changeToBlue();
        }
    }

    /**
     * Merge this state with the passed red by diverting all incoming links to the red state.
     *
     * @param redState
     */
    public void merge(State redState) {
        redState.mergeSketch(this.getSketch());
        redState.setCount(redState.getCount() + this.getCount());
        for (Symbol inLinkSymbol : this.inLinks.keySet()) {
            State origin = this.inLinks.get(inLinkSymbol);
            origin.setTransition(inLinkSymbol, redState);
        }
        // merge all white child states with the childs of the red state (or add new childs)
        mergeChilds(redState);
    }

    /**
     * Merge the childs of this state with the childs of the passed state that will remain to exist.
     */
    public void mergeChilds(State remainingState) {
        // loop through all childs of this state
        for (Symbol symbol : this.transitions.keySet()) {
            State child = getState(symbol);

            if (remainingState.hasTransition(symbol)) { // merge with corresponding child of the red state
                State remainingChild = remainingState.getState(symbol);
                remainingChild.mergeSketch(child.getSketch());
                remainingChild.setCount(child.getCount() + remainingChild.getCount());
                remainingState.transitionCounts.put(symbol, remainingState.transitionCounts.get(symbol) + this.transitionCounts.get(symbol));
                if (child.hasChildren()) {
                    // recursive call to merge all childs deeper in the tree than the first child
                    child.mergeChilds(remainingChild);
                }
            } else { // make this child a child of the red state
                // update depth
                child.setDepth(remainingState.getDepth() + 1);
                // set transition from red state to child
                remainingState.setTransition(symbol, child);
                // set transition count for the remaining state towards the newly added child state
                remainingState.transitionCounts.put(symbol, this.transitionCounts.get(symbol));
                // replace current child inlink by an inlink from red state
                child.addInLink(symbol, remainingState);
            }
        }
    }

    /**
     * Merge the sketch of this state with the given other sketch.
     *
     * @param other
     */
    public void mergeSketch(CountMinSketch other) {
        try {
            this.sketch = CountMinSketch.merge(this.sketch, other);
        } catch (Exception e) {
            System.out.println("Exception on merging sketches: " + e.getMessage());
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
        //double most_similar = 10000;
        State mostSimilarState = null;
        // find most similar state
        for (State other : otherStates) {
            double similarity = getSimilarity(other);
            if (similarity > most_similar) {
                mostSimilarState = other;
                most_similar = similarity;
            }
            /*
            double distance = getDistance(other);
            if (distance < most_similar) {
                mostSimilarState = other;
                most_similar = distance;
            }
            */
        }
        // return most similar state if it exceeds the boundary
        if (most_similar > COSINE_SIMILARITY) {
            return mostSimilarState;
        }
        /*
        if (most_similar < KL_DIVERGENCE) {
            return mostSimilarState;
        }
        */
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
    /*
    public boolean similarTo(State other) {
        return getDistance(other) < KL_DIVERGENCE;
    }
    */

    /**
     * Return the similarity of this state's sketch compared to the given other state's sketch.
     *
     * @param other
     * @return
     */
    public double getSimilarity(State other) {
        float[] sketch1 = this.getSketchVector();
        float[] sketch2 = other.getSketchVector();

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
        return similarity;
    }

    public double getDistance(State other) {
        double[] v1 = smoothen(this.getSketchVector());
        double[] v2 = smoothen(other.getSketchVector());

        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            sum += v1[i] * Math.log(v1[i] / v2[i]);
        }
        return sum;
    }

    public float[] getSketchVector() {
        float[] serialized_long = toLongArray(CountMinSketch.serialize(this.sketch));
        float[] vector = new float[DEPTH * WIDTH];
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
    protected float[] toLongArray(byte[] byteArray) {
        float[] longArray = new float[byteArray.length / 8];
        for (int i = 2; i < byteArray.length / 8; i++) {
            byte[] longBytes = Arrays.copyOfRange(byteArray, i*8, (i + 1)*8);
            ByteBuffer buffer = ByteBuffer.wrap(longBytes);
            longArray[i] = buffer.getLong() / (float) this.count;
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
    protected double chiSquare(float[] sketch1, float[] sketch2) {
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
    protected double cosineSimilarity(float[] v1, float[] v2) {
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
    protected double[] smoothen(float[] vector) {
        double[] smooth = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            smooth[i] = vector[i];
            if (vector[i] == 0) {
                smooth[i] = 0.0001;
            }
        }

        double sum = 0.0;
        for (int i = 0; i < smooth.length; i++) {
            sum += smooth[i];
        }

        double[] norm = new double[smooth.length];
        for (int i = 0; i < smooth.length; i++) {
            norm[i] = smooth[i] / sum;
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
            State next = getState(symbol);
            if (next.getColor().equals(Color.RED)) {
                count++;
            }
        }

        for (Symbol symbol : this.transitionCounts.keySet()) {
            State next = getState(symbol);
            if (next.getColor().equals(Color.BLUE)) {
                continue;
            }
            count++;
            double chance = 1.0 / (double) count;
            this.randomTransitions.add(chance, symbol);
        }

        // return a random Symbol
        if (count > 0) {
            return (Symbol) this.randomTransitions.randomEntry();
        }
        return null;
    }

    /**
     * The total number of transitions performed to all red states.
     *
     * @return
     */
    public int redStateTransitionCount() {
        int count = 0;
        for (Symbol symbol : this.transitionCounts.keySet()) {
            State next = getState(symbol);
            if (next.getColor().equals(Color.RED)) {
                count += this.transitionCounts.get(symbol);
            }
        }
        return count;
    }

    /**
     * Return the occurrence probability of the given symbol.
     *
     * @param symbol
     * @return
     */
    public Double getTransitionProbability(Symbol symbol) {
        if (this.transitionCounts.containsKey(symbol) && redStateTransitionCount() > 0) {
            return this.transitionCounts.get(symbol) / (double) redStateTransitionCount();
        } else {
            return null;
        }
    }

}
