package streaming;

import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import similarities.NormalizationFunction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.stream.IntStream;

public class TimeSeries {

//    Instance variables
    @NonNull public final int id;
    @NonNull @Getter private final String name;

//    Mini batch (aka basic window) related
    @NonNull @Getter private final double[] baseData;
    @NonNull @Getter private final int slidingWindowSize;
    @NonNull @Getter private final AggregationMethod aggMethod;
    @NonNull @Getter private final NormalizationFunction normFunction;


    //    Window of mini batches (normalized)
    @Getter private double[] slidingWindow;

    //    Window of mini batches (non-normalized), used to compute running sums
    @Getter private double[] slidingWindowRaw;

//    Number of elements currently in the mini batch
    @Getter private int batchCounter = 0;
    @Getter private int windowCounter = 0;

//    Similarity related (THIS IS OF NON-NORMALIZED DATA)
    @Setter @Getter private double runningSum;
    @Setter @Getter private double runningSumSq;

//    Of normalized data
    public double[] runningDots;

    public TimeSeries(int id, String name, double[] baseData, int slidingWindowSize, AggregationMethod aggMethod,
                      NormalizationFunction normFunction) {
        this.id = id;
        this.name = name;
        this.baseData = baseData;
        this.slidingWindowSize = slidingWindowSize;
        this.aggMethod = aggMethod;
        this.normFunction = normFunction;

        this.initSlidingWindow();
    }

    public String toString(){return String.valueOf(this.id);}
    public int hashCode(){return this.id;}

//    Initialize sliding window by taking the last w elements of the base data (and using them as the first mini batch values)
    public void initSlidingWindow(){
        int m = baseData.length;
        if (m < slidingWindowSize) {
            throw new IllegalArgumentException("The base data is too short for the given sliding window size.");
        }


        this.slidingWindow = new double[slidingWindowSize];
        this.slidingWindowRaw = new double[slidingWindowSize];

        int c = 0;
        for (int i = m - slidingWindowSize; i < m; i++) {
            double val = baseData[i];

//            Add value to end
            slidingWindow[c] = val;
            slidingWindowRaw[c] = val;
            c++;

//            Update running sums
            runningSum += val;
            runningSumSq += val * val;
        }

//        Normalize the sliding window
        this.normFunction.normalize(this);
    }

    public void initializeRunningDots(TimeSeries[] timeSeries){
        int n = timeSeries.length;
        runningDots = new double[n];



        for (int i=0; i<n; i++){
            runningDots[i] = lib.dot(this, timeSeries[i]);
        }
    }

//    Handle newly arrived data and return if the sliding window actually changed
    public boolean update(double val, TimeSeries[] timeSeries) {
        int lastAggIndex = (slidingWindowSize - 1 + windowCounter) % slidingWindowSize;

//        Get the oldest value in the raw sliding window
        double oldAggRaw = slidingWindowRaw[lastAggIndex];

//        Get new raw value
        double newAggRaw = aggMethod.update(oldAggRaw, val, batchCounter);
        slidingWindowRaw[lastAggIndex] = newAggRaw;

//        Update running statistics
        runningSum += (newAggRaw - oldAggRaw);
        runningSumSq += (newAggRaw * newAggRaw - oldAggRaw * oldAggRaw);

//        Now update the normalized sliding window
        double oldAgg = slidingWindow[lastAggIndex];
        double avg = runningSum / slidingWindowSize;
        double std = Math.sqrt(runningSumSq / slidingWindowSize - avg * avg);
        double newAgg = (newAggRaw - avg) / std;

        slidingWindow[lastAggIndex] = newAgg;
        batchCounter++;

        double delta = newAgg - oldAgg;
        boolean changed = Math.abs(delta) > 1e-6;

//        Update running statistics
        if (changed) {
//            Update dots
            for (int i = 0; i < timeSeries.length; i++) {
                TimeSeries ts = timeSeries[i];

                if (ts.id == this.id) {
                    continue;
                }

                double otheragg = ts.slidingWindow[lastAggIndex];
                double dotChange = delta * otheragg;

                runningDots[i] += dotChange;
                ts.runningDots[id] += dotChange;
            }
        }

        return changed;
    }

//    Update the sliding window without updating the running dots, return old and new values
    public boolean slideWindow(){
        int newAggIndex = windowCounter % slidingWindowSize;

//        Get the oldest value in the raw sliding window
        double oldAggRaw = slidingWindowRaw[newAggIndex];

//        Get new raw value
        double newAggRaw = aggMethod.reset(oldAggRaw);

        boolean changed = Math.abs(newAggRaw - oldAggRaw) > 1e-6;

        if (changed){
            slidingWindowRaw[newAggIndex] = newAggRaw;

//        Update running statistics
            runningSum += (newAggRaw - oldAggRaw);
            runningSumSq += (newAggRaw * newAggRaw - oldAggRaw * oldAggRaw);

//        Now update the normalized sliding window and renormalize
            slidingWindow = slidingWindowRaw.clone();

//        Renormalize the sliding window
            this.normFunction.normalize(this);
        }

        windowCounter++;
        batchCounter = 0;
        return changed;
    }
}


