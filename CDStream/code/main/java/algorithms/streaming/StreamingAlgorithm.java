package algorithms.streaming;

import _aux.*;
import _aux.lists.FastLinkedList;
import algorithms.Algorithm;
import algorithms.AlgorithmEnum;
import algorithms.statics.SimilarityDetective;
import bounding.ClusterCombination;
import bounding.RecursiveBounding;
import core.Parameters;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import queries.ProgressiveStopException;
import queries.QueryTypeEnum;
import queries.ResultSet;
import queries.RunningThreshold;
import streaming.Arrival;
import streaming.BatchModelEnum;
import streaming.LinearRegressor;
import streaming.TimeSeries;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

public abstract class StreamingAlgorithm extends Algorithm {
    @Setter protected LinearRegressor runtimePredictor = new LinearRegressor();

//    Attribute indicating at which window the algorithm is currently
    @Setter @Getter protected int globalWindowIndex = 1;

    protected StageRunner[] stageRunners;

    //    Bitset indicating which timeseries changed this epoch and therefore need to be handled
    @Getter protected Set<TimeSeries> updatedTS;
    protected int nArrivals = 0;

//    Used for measuring changes in similarities
    public double[][] oldDistances;
    public SimilarityDetective SD;
    @Getter protected ResultSet resultSet;

    protected boolean windowsSlid = false;


    public StreamingAlgorithm(@NonNull Parameters parameters) {
        super(parameters);

        //        Create a stageRunner for each epoch (to measure the duration of each stage)
        stageRunners = new StageRunner[par.epochs];
        updatedTS = new HashSet<>(par.n);

//        Create a SD instance for initiating the first epoch
        SD = new SimilarityDetective(par);
    }

    public abstract ResultSet run();

    public void simulate(){
        par.LOGGER.info("---------------- Running one-shot CD to initialize ----------------");
        par.statBag.stopWatch.reset();
        par.statBag.stopWatch.start();

//        Initialize the algorithm by running one-shot first
        resultSet = SD.run();

//        First statistics
        par.statBag.getMiscStats().put("oneshotDuration", lib.nanoToSec(par.statBag.stopWatch.getNanoTime()));
        par.statBag.getMiscStats().put("oneshotResults", resultSet.size());
        par.statBag.stopWatch.reset();

//        Write out first resultset
        writeResults();

//        Simulate the algorithm for the specified number of epochs
        while (par.epoch < par.epochs){
            par.epoch++;
            par.LOGGER.info("---------------- Simulating epoch " + par.epoch + " ----------------");

//            Create new stageRunner
            StageRunner stageRunner = new StageRunner(par.getLOGGER());
            stageRunners[par.epoch-1] = stageRunner;

//            Reset updated TS indicator
            updatedTS.clear();

//            Check if we should start artificial arrival wave
            checkArrivalWave();

            par.statBag.stopWatch.start();

//            STAGE 1: SLIDE WINDOWS, FETCH ARRIVALS FROM QUEUE, AND UPDATE TIMESERIES
            nArrivals = stageRunner.run("Sliding windows and Fetching arrivals", this::fetchAndSlide, par.statBag.stopWatch);
            par.getLOGGER().fine("Number of arrivals: " + nArrivals);

            if (updatedTS.size() > 0) {
//            STAGE 4: UPDATE SIMILARITIES FOR TS THAT CHANGED
                stageRunner.run("Updating similarities", this::updateDistances, par.statBag.stopWatch);

//            STAGE 5: UPDATE TOPK (only if streaming)
                if (par.queryType == QueryTypeEnum.TOPK) {
                    stageRunner.run("Updating topk", this::updateTopK, par.statBag.stopWatch);
                }

//            STAGE 6: UPDATE RESULTS
                par.simMetric.clearCache();
                this.resultSet = stageRunner.run("Updating results", this::run, par.statBag.stopWatch);
            }

//            Log results
            par.LOGGER.info(String.format("Current result set size %s: %d",
                    par.queryType.equals(QueryTypeEnum.TOPK) ? "(includes topk buffer)": "", resultSet.size()));

//            Update statistics
            par.statBag.getActivatedAlgorithms()[par.epoch-1] = par.activeAlgorithm;
            par.statBag.getDurations()[par.epoch-1] = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
            par.statBag.getResultCounts()[par.epoch-1] = resultSet.size();
            par.statBag.getArrivalCounts()[par.epoch-1] = nArrivals;
            par.statBag.getSimulatedTimes().add(par.time);
            par.statBag.getDccCounts()[par.epoch-1] = par.statBag.getNIndexedDCCs().get();
            par.statBag.getDccSizes()[par.epoch-1] = par.statBag.getTotalIndexedDCCsSize().get();

//            Write out results
            writeResults();

//            Force run GC every 10 epochs
            if (par.epoch % 10 == 0){
                System.gc();
            }

            par.statBag.stopWatch.reset();
        }

        closeResults();
        printStats(par.statBag);

        if (par.saveStats){
            par.statBag.saveStats(par);
        }
    }

    //        Update time based on epoch size make sure that windows are slid at right points
    public int fetchAndSlide(){
        int nArrivals = 0;

//        Loop until we reached end of epoch
        if (par.batchModel.equals(BatchModelEnum.TIMEBASED)){
            double epochEndTime = par.time + par.timePerEpoch;

            while (par.time < epochEndTime){
                int windowEndTime = globalWindowIndex * par.basicWindowSize;

//            Check if epoch falls within window
                if (epochEndTime < windowEndTime){
                    par.time = epochEndTime;

//                Get arrivals up to window slide and update timeseries
                    nArrivals += fetchArrivals(0);
                } else {
                    par.time = windowEndTime;

//                Get arrivals up to window slide
                    nArrivals += fetchArrivals(0);

//                Slide windows
                    slideWindows();
                }
            }
        } else if (par.batchModel.equals(BatchModelEnum.COUNTBASED)) {
            while (nArrivals < par.arrivalBatchSize) {
                int windowEndTime = globalWindowIndex * par.basicWindowSize;
                par.time = windowEndTime;

                nArrivals += fetchArrivals(nArrivals);

//                If arrivals are not enough, slide windows and fetch more
                if (nArrivals < par.arrivalBatchSize) {
                    slideWindows();
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown batch model");
        }

        return nArrivals;
    }


    public void slideWindows(){
        par.getLOGGER().warning("Sliding all windows to window " + (globalWindowIndex+1));

        lib.getStream(par.timeSeries, par.parallel).forEach(t -> {
            boolean changed = t.slideWindow();
            if (changed){
                updatedTS.add(t);
            }
        });

//            Update dots
        lib.getStream(par.timeSeries, par.parallel).forEach(t -> {
            t.initializeRunningDots(par.timeSeries);
        });

        windowsSlid = true;
        globalWindowIndex++;
    }

//    Get arrivals up until current time and aggregate by time series
    public int fetchArrivals(int currentNArrivals){
        HashMap<Integer, Double> aggArrivals = new HashMap<>();
        int nArrivals = 0;

        switch (par.batchModel){
            case TIMEBASED: {
                Arrival queuePointer = par.arrivalQueue.peek();

//                Fetch all arrivals that have a timestamp smaller than the current time
                while (queuePointer != null && queuePointer.t <= par.time){
                    Arrival arrival = par.arrivalQueue.poll();
                    nArrivals++;

//                    Aggregate updates by time series
                    aggArrivals.merge(arrival.key, arrival.val,
                            (oldVal, newVal) -> par.getBasicWindowAggMethod().update(oldVal, newVal, 0));

                    queuePointer = par.arrivalQueue.peek();
                }
                break;
            }
            case COUNTBASED: {
//                Fetch the specified number of arrivals
                for (int i = currentNArrivals; i < par.arrivalBatchSize; i++){
                    if (par.arrivalQueue.isEmpty()){
                        break;
                    }
                    Arrival arrival = par.arrivalQueue.peek();

//                    Check if arrival is within the current time
                    if (arrival.t > par.time){
                        break;
                    }

                    arrival = par.arrivalQueue.poll();
                    nArrivals++;

                        //                    Aggregate updates by time series
                    aggArrivals.merge(arrival.key, arrival.val,
                            (oldVal, newVal) -> par.getBasicWindowAggMethod().update(oldVal, newVal, 0));
                }
                break;
            }
            default: {
                throw new RuntimeException("Unknown batch model");
            }
        }

//        Update time series
        updateTS(aggArrivals);

        return nArrivals;
    }

    public void updateTS(HashMap<Integer, Double> aggArrivals){
//        Update the time series with the aggregated arrivals
        aggArrivals.forEach((tsId, aggVal) -> {
            TimeSeries ts = par.timeSeries[tsId];
            if (ts.update(aggVal, par.timeSeries)) updatedTS.add(par.timeSeries[tsId]);
        });
    }

    public void updateDistances(){
//            Set old distances to measure changes
        oldDistances = lib.deepCopy(par.getPairwiseDistances());

//            Iterate over updateTS
        lib.getStream(updatedTS, par.parallel).forEach(ts -> {
            int tsId = ts.id;
            lib.getStream(IntStream.range(0, par.n).boxed(), par.parallel).forEach(otherTsId -> {
                if (tsId != otherTsId){
                    TimeSeries otherTs = par.timeSeries[otherTsId];
                    double newDist = par.simMetric.distFunc.dist(ts, otherTs);
                    par.pairwiseDistances[tsId][otherTsId] = newDist;
                    par.pairwiseDistances[otherTsId][tsId] = newDist;
                }
            });
        });
    }

    public void updateTopK() {
//        Only update topk if the algorithm is iterative (i.e. not one-shot)
        if (! (this instanceof SDOneShot)){
//            Clear and update all ccs in topk + buffer
            ArrayList<ClusterCombination> oldTopK = new ArrayList<>(resultSet.getResults());
            resultSet.clear();
//            Reset threshold so it's updated correctly
            par.runningThreshold = new RunningThreshold(0, par.getLOGGER());

            try{
                RecursiveBounding.runCCs(oldTopK, par, 1, resultSet);
            } catch (ProgressiveStopException ignored) {}

//            Update threshold to new value
            PriorityQueue<ClusterCombination> resultSet = (PriorityQueue<ClusterCombination>) this.resultSet.getResults();
            par.runningThreshold.setThreshold(resultSet.peek().getLB());
        }
    }

    public void checkArrivalWave(){
//        Check if we should start the arrival wave (midway in non-training phase)
        int midRun = par.warmupEpochs + (par.epochs / 2);
        int arrivalWaveStart = midRun - (par.boostEpochs / 2);
        int arrivalWaveEnd = midRun + (par.boostEpochs / 2);
        if (par.epoch == arrivalWaveStart){
            par.getLOGGER().warning("Arrival wave started");
            par.timePerEpoch *= par.boostFactor;
        }

//        Check if we should stop the arrival wave (end of non-training phase)
        if (par.epoch == arrivalWaveEnd){
            par.getLOGGER().warning("Arrival wave ended");
            par.timePerEpoch /= par.boostFactor;
        }
    }

//    ------------------------------ statistics methods ------------------------------
    @Override
    public void prepareStats(){
//        Get avg dcc size per epoch
        double[] avgDccSizes = new double[par.statBag.getDccSizes().length];
        for (int i = 0; i < avgDccSizes.length; i++){
            avgDccSizes[i] = par.statBag.getDccSizes()[i] / ((double) par.statBag.getDccCounts()[i] + 1);
        }

//      Aggregate the stageduration times together
        ArrayList<Stage> globalStageDurations = new ArrayList<>();
        ArrayList<Double> epochsPerStage = new ArrayList<>();
        for (int i = 0; i < stageRunners.length; i++) {
            StageRunner stageRunner = stageRunners[i];
            if (stageRunner == null) continue;

            int j = 0;
            for (Stage stage: stageRunner.stageDurations) {
//                Add if necessary
                double duration = stage.duration;
                if (globalStageDurations.size() <= j){
                    globalStageDurations.add(new Stage(stage.name, duration));
                    epochsPerStage.add(1.0);
                } else {
//                    Add to existing
                    globalStageDurations.get(j).duration += duration;
                    epochsPerStage.set(j, epochsPerStage.get(j) + 1);
                }
                j++;
            }
        }

//        Average the stage durations
        for (int i = 0; i < globalStageDurations.size(); i++){
            globalStageDurations.get(i).duration /= epochsPerStage.get(i);
        }
        par.statBag.stageDurations = new FastLinkedList<>(globalStageDurations);

//        Prepare the epoch averages
        par.LOGGER.fine("Preparing epoch averages, warmupStop value is " + par.warmupStop);
        par.statBag.getMiscStats().put("avgDuration", lib.arrayAvg(Arrays.copyOfRange(par.statBag.getDurations(), par.warmupStop, par.epochs)));
        par.statBag.getMiscStats().put("avgArrivals", lib.arrayAvg(Arrays.copyOfRange(par.statBag.getArrivalCounts(), par.warmupStop, par.epochs)));
        par.statBag.getMiscStats().put("avgResults", lib.arrayAvg(Arrays.copyOfRange(par.statBag.getResultCounts(), par.warmupStop, par.epochs)));
        par.statBag.getMiscStats().put("avgDccCount", lib.arrayAvg(Arrays.copyOfRange(par.statBag.getDccCounts(), par.warmupStop, par.epochs)));
        par.statBag.getMiscStats().put("avgDccSize", lib.arrayAvg(avgDccSizes));
        par.statBag.getMiscStats().put("avgViolationCount", lib.arrayAvg(Arrays.copyOfRange(par.statBag.getViolationCounts(), par.warmupStop, par.epochs)));
    }

    @Override
    public void printStats(StatBag statBag){
        this.prepareStats();

//        Oneshot stats
        par.LOGGER.fine("----------- Oneshot run statistics --------------");
        par.LOGGER.fine(String.format("%-30s %.2f","duration:", (double) par.statBag.getMiscStats().get("oneshotDuration")));
        par.LOGGER.fine(String.format("%-30s %d","nResults:", (int) par.statBag.getMiscStats().get("oneshotResults")));
        par.LOGGER.fine(String.format("%-30s %d","nLookups:", par.statBag.getNLookups().get()));
        par.LOGGER.fine(String.format("%-30s %d","nCCs:", par.statBag.getNCCs().get()));
        par.LOGGER.fine(String.format("%-30s %d","nSeqCCs:", par.statBag.getNSecCCs().get()));
        par.LOGGER.fine(String.format("%-30s %d","nParallelCCs:", par.statBag.getNParallelCCs().get()));
        par.LOGGER.fine(String.format("%-30s %.1f","avgCCSize:",
                (double) par.statBag.getTotalCCSize().get() / (double) par.statBag.getNCCs().get()));

        lib.printBar(par.LOGGER);

//        Streaming stats
        par.LOGGER.fine("----------- Streaming run statistics (avg over epochs) --------------");
//        Durations and arrivals
        par.LOGGER.fine(String.format("%-30s %.5f","duration:", (double) par.statBag.getMiscStats().get("avgDuration")));
        par.LOGGER.fine(String.format("%-30s %.5f","arrivals:", (double) par.statBag.getMiscStats().get("avgArrivals")));
        par.LOGGER.fine(String.format("%-30s %.5f","results:", (double) par.statBag.getMiscStats().get("avgResults")));

//        DCCs
        par.LOGGER.fine(String.format("%-30s %.5f","dccCount:", (double) par.statBag.getMiscStats().get("avgDccCount")));
        par.LOGGER.fine(String.format("%-30s %.5f","dccSize:", (double) par.statBag.getMiscStats().get("avgDccSize")));
        par.LOGGER.fine(String.format("%-30s %.5f","violations:", (double) par.statBag.getMiscStats().get("avgViolationCount")));

        this.printStageDurations(statBag);
    }

    public void writeResults(){
        if (par.saveResults){
            par.LOGGER.info("Writing results to file");
            resultSet.writeOut(par.resultWriter, par.epoch, par.timeSeries);
        }
    }

    public void closeResults(){
        if (par.saveResults){
            try {
                par.resultWriter.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
