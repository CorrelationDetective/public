package streaming;

import _aux.Key;
import _aux.Pair;
import _aux.Tuple3;
import _aux.lib;
import bounding.CorrelationBounding;
import bounding.HierarchicalBounding;
import clustering.Cluster;
import enums.AlgorithmEnum;
import hybrid.LinearRegressor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StreamingDetective {
    private TimeSeries[] timeSeries;
    private HierarchicalBounding HB;


//  Misc
    public int seed;
    public long jvmStartTime;
    public int cores;

    //    Simulation attributes
    public PriorityQueue<Pair<Integer, TimeSeries>> arrivalQueue;
    private boolean parallel;
    public double epochSize;
    public int t;

    //    Run statistics
    public final boolean saveStats;
    public ArrayList<Integer> violationCounts;
    public ArrayList<Double> avgGroupingSize;
    public ArrayList<Integer> falseViolations;
    public ArrayList<Integer> arrivalCount;
    public ArrayList<Double> durationArray;
    public ArrayList<Integer> dccCounts;
    public ArrayList<Integer> dccSizes;
    public ArrayList<Double> timestampArray;
    public ArrayList<Double> clusterDiff;
    public ArrayList<Integer> simTimes;
    public ArrayList<String> algorithmUsed;
    public ArrayList<Double> conflictSolvedPercentage;

//    Hybrid variables
    public AlgorithmEnum generalAlgorithm;
    public AlgorithmEnum runningAlgorithm;
    public Set<int[]> oldResultSet;
    public CorrelationBounding oldCB;
    public int oldDCCCount;
    public int lastSwitch;

    public Set<DCC> newPositives;
    public Set<DCC> newNegatives;
    public Set<Double[]> resultSetForSaving;
    public BitSet ubGroupingFilteredAfterSwitch;
    public BitSet lbGroupingFilteredAfterSwitch;

    public LinearRegressor streamingRegressor = new LinearRegressor();
    public LinearRegressor oneshotRegressor = new LinearRegressor();
    public int warmupPeriod;
    HashMap<String, Object> parameters;


    public StreamingDetective(HashMap<String, Object> parameters){
        this.parameters = parameters;

        this.timeSeries = (TimeSeries[]) parameters.get("timeSeries");
        this.HB = (HierarchicalBounding) parameters.get("HB");
        this.warmupPeriod = (int) parameters.get("warmupPeriod");
        this.epochSize = 1;

        this.saveStats = (boolean) parameters.get("saveStats");

        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        this.jvmStartTime = bean.getStartTime();
        this.cores = Runtime.getRuntime().availableProcessors();

        generalAlgorithm = (AlgorithmEnum) parameters.get("algorithm");
        runningAlgorithm = (AlgorithmEnum) parameters.get("runningAlgorithm");

    }

    public ArrayList<Double> simulate() {
        int epochs = (int) parameters.get("epochs");
        this.parallel = (boolean) parameters.get("parallel");
        this.violationCounts = new ArrayList<>(epochs);
        this.avgGroupingSize = new ArrayList<>(epochs);
        this.arrivalCount = new ArrayList<>(epochs);
        this.durationArray = new ArrayList<>(epochs);
        this.dccCounts = new ArrayList<>(epochs);
        this.dccSizes = new ArrayList<>(epochs);
        this.timestampArray = new ArrayList<>(epochs);
        this.clusterDiff = new ArrayList<>(epochs);
        this.falseViolations = new ArrayList<>(epochs);
        this.simTimes = new ArrayList<>(epochs);
        this.algorithmUsed = new ArrayList<>(epochs);
        this.conflictSolvedPercentage = new ArrayList<>(epochs);

        ArrayList<Double> durations = new ArrayList<>();

        this.seed = (int) parameters.get("seed");
        Integer boostFactor = (Integer) parameters.get("boostFactor");
        Integer boostPeriod = (Integer) parameters.get("boostPeriod");

//        Schedule all epochs before simulating
        this.arrivalQueue = scheduleArrivals(timeSeries);

        int boostStart = warmupPeriod*2 + ((epochs - warmupPeriod*2) / 2) - boostPeriod/2;
        int boostEnd = warmupPeriod*2 + ((epochs - warmupPeriod*2) / 2) + boostPeriod/2;


//        Simulate
        while (HB.epoch < epochs) {
            HB.epoch++;

//            Check if at start or end of boosting period
            if (HB.epoch == boostStart){
                System.out.println("Boost period started");
                this.epochSize = boostFactor;
            } else if (HB.epoch == boostEnd){
                System.out.println("Boost period ended");
                this.epochSize = 1;
            }

            this.t += epochSize;
            this.simTimes.add(this.t);

//            Get arrivals
            List<TimeSeries> arrivals = new ArrayList<>();
            Pair<Integer, TimeSeries> queuePointer = arrivalQueue.peek();

            while (queuePointer != null && queuePointer.x <= this.t) {
                Pair<Integer, TimeSeries> arrival = arrivalQueue.poll();
                if (arrival == null) {
                    break;
                } else {
                    arrivals.add(arrival.y);

//                    Progress window to point
                    try{
                        arrival.y.progressWindow(arrival.x);
                    } catch (IllegalStateException ignored){continue;}
                    queuePointer = arrivalQueue.peek();
                }
            }

            System.out.println("epoch " + HB.epoch + ", time = " + this.t + ", arrivals: " + arrivals.size());


            long cycleStart = System.currentTimeMillis();
            timestampArray.add(StreamLib.getDuration(jvmStartTime, System.currentTimeMillis()));

            double dur;

            if (arrivals.size() > 0){

                //            First do warmup period of 60 epochs
                if (generalAlgorithm.equals(AlgorithmEnum.HYBRID) && HB.epoch == warmupPeriod){
                    System.out.println("Switching to oneshot - part of warmup period");
                    switchToOneshot();
                }

////            After that, choose which algorithm going to use. Set slack of performanceWindow to limit the amount of switches
                if (generalAlgorithm.equals(AlgorithmEnum.HYBRID) && HB.epoch > 2*warmupPeriod && HB.epoch - lastSwitch > warmupPeriod){
                    chooseAlgorithm(arrivals.size());
                }

                arrival(new HashSet<>(arrivals));

                dur = StreamLib.getDuration(cycleStart, System.currentTimeMillis());

//                Update duration predictors
                if (HB.epoch - lastSwitch > 5){
                    LinearRegressor lr = runningAlgorithm.equals(AlgorithmEnum.STREAMING) ? streamingRegressor: oneshotRegressor;
                    lr.update(arrivals.size(), dur);
                }
            } else {
                dur = 0;
                arrivalCount.add(0);
            }

            durations.add(dur);
            durationArray.add(dur);

            dccCounts.add(HB.dccCount);
            dccSizes.add(HB.totalDCCSize);
            algorithmUsed.add(runningAlgorithm.toString());
        }

        if (this.saveStats){
            this.saveStats();
        }

        return durations;
    }

    public void arrival(Set<TimeSeries> arrivals){
        if (saveStats){arrivalCount.add(arrivals.size());}


        if (runningAlgorithm.equals(AlgorithmEnum.STREAMING)){
            if (lastSwitch != HB.epoch){
                //        Set old correlations
                lib.getStream(timeSeries, parallel).forEach(stock -> stock.oldCorrelations = stock.pairwiseCorrelations.clone());
            }

            //        Update correlation cache for that stock
            lib.getStream(arrivals, parallel).forEach(stock -> stock.computePairwiseCorrelations(timeSeries, true));

//        Get violations
            ConcurrentMap<Object, List<DCC>> violations = lib.getStream(arrivals, parallel)
                    .flatMap(stock -> {
                        ArrayList<DCC> vios = new ArrayList<>();
                        vios.addAll(pruneGrouping(stock, arrivals));
                        return vios.stream();
                    })
                    .collect(Collectors.toSet())
                    .stream()
                    .collect(Collectors.groupingByConcurrent(i -> Math.abs(HB.RC.random.nextInt()) % cores));

//            Handle breaks
            lib.getStream(violations.values(), parallel)
                    .forEach(dccList -> dccList.forEach(this::handleLevel2Violation));

        } else if (runningAlgorithm.equals(AlgorithmEnum.ONESHOT)){
            parameters.put("t", HB.epoch);
            HierarchicalBounding HB = new HierarchicalBounding(parameters);
            this.HB = HB;
            HB.recursiveBounding(parameters);
        } else {
            throw new IllegalStateException("runningAlgorithm is " + runningAlgorithm.toString() + ". This should not happen");
        }
    }

    private List<DCC> pruneGrouping(TimeSeries ts, Set<TimeSeries> arrivals) throws RuntimeException {
        IntStream indexStream = IntStream.range(0, timeSeries.length);

        indexStream = parallel ? indexStream.parallel(): indexStream.sequential();

        return indexStream.mapToObj(otherId -> {
            ArrayList<DCC> localViolations = new ArrayList<>();

//            Skip reflexive pairs
            if (ts.id == otherId){return localViolations.stream();}

//            Stock pair will already be investigated by other thread
            if (arrivals.contains(this.timeSeries[otherId]) && otherId > ts.id){
                return localViolations.stream();
            }


            double newcorr = ts.pairwiseCorrelations[otherId];
            double oldcorr = ts.oldCorrelations[otherId];
            double deltacorr = newcorr - oldcorr;

            boolean ub = deltacorr > 0;

            BitSet groupingFilteredAfterSwitch = ub ? ubGroupingFilteredAfterSwitch: lbGroupingFilteredAfterSwitch;

            ArrayList<Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>>> grouping = ub ? HB.ubGrouping: HB.lbGrouping;

            Key idxKey = ts.id < otherId ? new Key(ts.id, otherId): new Key(otherId, ts.id);
            int groupingIndex = StreamLib.getGroupIndex(idxKey.x, idxKey.y, timeSeries.length);

            Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>> node = grouping.get(groupingIndex);

            List<Key> removeGroups = new ArrayList<>();
            Tuple3<List<DCC>, List<DCC>, List<DCC>> addLaters = new Tuple3<>(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

            Set<Key> bdps = node.keySet();

//                Iterate over second level grouping
            for (Key BDP : bdps) {
                double thCorr = idxKey.equals(BDP) ? timeSeries[BDP.x].oldCorrelations[BDP.y] :
                        timeSeries[BDP.x].pairwiseCorrelations[BDP.y];

//                If newCorr exceeds threshold check stored DCCs
                if ((ub && newcorr > thCorr) || (!ub && newcorr < thCorr)) {
                    Tuple3<List<DCC>, List<DCC>, List<DCC>> partitionedDccList = node.get(BDP);

                    if (partitionedDccList == null) {
                        continue;
                    }

//                    If empty group, delete from node to save time
                    if (partitionedDccList.x.size() + partitionedDccList.y.size() == 0) {
                        removeGroups.add(BDP);
                        continue;
                    }

                    List<DCC> posDccs = partitionedDccList.x;
                    List<DCC> negDccs = partitionedDccList.y;
                    List<DCC> posMinJumpDccs = partitionedDccList.z;

//                    Lazy filtering of dead DCCs
                    List<DCC> posAliveDccs = posDccs.stream().filter(dcc -> dcc != null && dcc.isAlive() && dcc.boundTuple.state == 1)
                            .collect(Collectors.toList());

                    List<DCC> negAliveDccs = negDccs.stream().filter(dcc -> dcc != null && dcc.isAlive() && dcc.boundTuple.state == -1)
                            .collect(Collectors.toList());

                    posMinJumpDccs = posMinJumpDccs == null ? null :
                            posMinJumpDccs.stream().filter(dcc -> dcc != null && dcc.isAlive() && dcc.boundTuple.state == 1).collect(Collectors.toList());

                    //                Filter out group conflicts due to algorithm switch
                    if (groupingFilteredAfterSwitch != null && !groupingFilteredAfterSwitch.get(ts.id)){
//                    Handle dccs with conflicts
                        posAliveDccs = this.getAndSolveConflicts(posAliveDccs, newNegatives);

//                    Do the same for negatives
                        negAliveDccs = this.getAndSolveConflicts(negAliveDccs, newPositives);

//                    Set cell to true
                        groupingFilteredAfterSwitch.set(ts.id);
                    }

//                            idKey is now BDP, move later
                    if (!BDP.equals(idxKey)){
                        addLaters.x.addAll(posAliveDccs);
                        addLaters.y.addAll(negAliveDccs);
                        if (posMinJumpDccs != null){
                            addLaters.z.addAll(posMinJumpDccs);
                        }

//                            Remove current node to save time later
                        removeGroups.add(BDP);
                    }
//                        Current idKey is already BDP
                    else {
                        partitionedDccList.x = posAliveDccs;
                        partitionedDccList.y = negAliveDccs;
                        partitionedDccList.z = posMinJumpDccs;
                    }

                    localViolations.addAll(posAliveDccs);
                    localViolations.addAll(negAliveDccs);

//                        Add minJump DCCs to violations if the new correlation might break some positive DCCs
                    if (ub && newcorr > HB.tau - HB.minJump && posMinJumpDccs != null){
                        localViolations.addAll(posMinJumpDccs);
                    }
                }
            }
//                Add violated BDP to new BDP (which is idKey)
            node.merge(idxKey, addLaters, (old, now) -> {
                old.x.addAll(now.x);
                old.y.addAll(now.y);
                return old;
            });

            removeGroups.forEach(node::remove);
            return localViolations.stream();
        }).flatMap(stream -> stream).collect(Collectors.toList());
    }

    private List<DCC> getAndSolveConflicts(List<DCC> dccList, Set<DCC> conflictList){
        return lib.getStream(dccList, parallel).map(dcc -> {
            // Iterate over new negatives to see if this dcc is a superset of any of the new negatives
            List<DCC> conflicts = conflictList.stream()
                    .filter(newDcc -> newDcc.isSubsetOf(dcc))
                    .collect(Collectors.toList());
//                            If there is a subset of this DCC in the new negatives, split that dcc off and go on.
            if (conflicts.size() > 0){
//                Kill dcc so it's picked up by GC and is not grouped again
                dcc.dead = true;
                DCC newDcc = dcc;
                for (DCC conflict: conflicts){
//                    Check if dcc is really subset, if not, just delete dcc and use new one.
                    if (conflict.getSize() == dcc.getSize()){
                        continue;
                    } else {
                        newDcc = newDcc.splitOff(conflict, timeSeries, HB.epoch, HB.CB);
                        newDcc.lastUpdateTime = -1; // set to be able to partition
                    }
                }
                return newDcc;
            } else {
                return dcc;
            }
        }).collect(Collectors.toList());
    }

    private void handleLevel2Violation(DCC dcc) throws IllegalStateException {
//        Check again if corrbound is violated, could have been updated in the meantime
        if (dcc.dead || dcc.lastUpdateTime == HB.epoch){
            return;
        }

        int oldState = dcc.boundTuple.state;

//            Compute new bound after violation
        dcc.boundTuple = HB.CB.calcBound(dcc.LHS, dcc.RHS);

        int newState = dcc.boundTuple.state;

//        Always regroup if lastupdateTime is negative. This means the dcc is newly created
        if (dcc.lastUpdateTime < 0){
            HB.groupDCC(dcc);
            dcc.lastUpdateTime = HB.epoch;
            return;
        }

        dcc.lastUpdateTime = HB.epoch;

        if (newState == oldState && saveStats){
            if (falseViolations.size() > HB.epoch){
                try{
                    falseViolations.set(HB.epoch, falseViolations.get(HB.epoch) + 1);
                } catch (NullPointerException | ArrayIndexOutOfBoundsException ignored){}
            } else {
                falseViolations.add(1);
            }
        }

        if (newState == 0) {
                if (oldState == 1){
                    HB.positiveDCCs.remove(dcc);
                }

                if (violationCounts != null && saveStats){
                    if (violationCounts.size() > HB.epoch){
                        try{
                            violationCounts.set(HB.epoch, violationCounts.get(HB.epoch) + 1);
                        } catch (NullPointerException ignored){}
                    } else {
                        violationCounts.add(1);
                    }
                }

//            Disable DCC
                dcc.dead = true;
                HB.dccCount--;

                //            Split DCC and group
                HB.breakUCC(dcc.LHS, dcc.RHS);


        } else if (newState != oldState){

//            Pos to neg
            if (newState == -1){
                HB.positiveDCCs.remove(dcc);
            } else {
//                Neg to pos
                HB.positiveDCCs.add(dcc);
            }

//                Group again
            HB.groupDCC(dcc);
        }
    }



    private void chooseAlgorithm(int arrivalCount){
        double streamPrediction = lib.round(streamingRegressor.predict(arrivalCount), 2);
        double oneshotPrediction = lib.round(oneshotRegressor.predict(arrivalCount), 2);

        System.out.println("oneshotPrediction=" + oneshotPrediction
                + " streamPrediction=" + streamPrediction);

        if (runningAlgorithm.equals(AlgorithmEnum.STREAMING) && oneshotPrediction < streamPrediction){
            switchToOneshot();
        } else if (runningAlgorithm.equals(AlgorithmEnum.ONESHOT) && streamPrediction < oneshotPrediction){
            switchToStreaming();
        }
        
//         Run GC
        System.gc();
    }

    private void switchToOneshot() {
        if (runningAlgorithm.equals(AlgorithmEnum.ONESHOT)) {
            System.out.println("Asked to switch to oneshot but already in oneshot");
            return;
        }

        System.out.println("Switching to oneshot!");

        this.lastSwitch = HB.epoch;

        this.runningAlgorithm = AlgorithmEnum.ONESHOT;
        parameters.put("runningAlgorithm", runningAlgorithm);

//      Set old resultSet to find differences later
        this.oldResultSet = HB.resultSet;
        this.oldCB = HB.CB;
        this.oldDCCCount = HB.dccCount;


        //        Set old correlations
        lib.getStream(timeSeries, parallel).forEach(stock -> stock.oldCorrelations = stock.pairwiseCorrelations.clone());
    }

    private void switchToStreaming(){
        if (runningAlgorithm.equals(AlgorithmEnum.STREAMING)){
            System.out.println("Asked to switch to streaming but already in streaming");
            return;
        }

        System.out.println("Switching to streaming!");

        this.runningAlgorithm = AlgorithmEnum.STREAMING;
        parameters.put("runningAlgorithm", runningAlgorithm);

//        Use new CB again
        this.HB.CB = oldCB;
        this.HB.dccCount = oldDCCCount;

//        Initiate groupFilterAfterSwitch bitmap
        ubGroupingFilteredAfterSwitch = new BitSet(timeSeries.length);
        lbGroupingFilteredAfterSwitch = new BitSet(timeSeries.length);

        this.lastSwitch = HB.epoch;

//      Get old resultSet to find difference
        if (this.oldResultSet == null){
            throw new IllegalStateException("No old resultSet saved!");
        }


//        Reset newDCC datastructures
        newPositives = new HashSet<>();
        newNegatives = new HashSet<>();

//        Get DCCs which moved into resultSet
        Pair<List<int[]>, List<int[]>> diffPair = StreamLib.symmetricDifference(new ArrayList<>(HB.resultSet), new ArrayList<>(oldResultSet));

        List<int[]> toChange = new ArrayList<>(diffPair.x); toChange.addAll(diffPair.y);

//        Create new DCCs from differences
        lib.getStream(toChange, parallel).forEach(ids -> {
//            Make LSH
            ArrayList<Cluster> newLSH = new ArrayList<>(IntStream.range(0, HB.pLeft).mapToObj(i -> {
                int sid = ids[i];
                return new Cluster(-sid, timeSeries[sid], HB.epoch, timeSeries.length);
            }).collect(Collectors.toList()));

//            Make RSH
            ArrayList<Cluster> newRSH = new ArrayList<>(IntStream.range(HB.pLeft, HB.pLeft + HB.pRight).mapToObj(i -> {
                int sid = ids[i];
                return new Cluster(-sid, timeSeries[sid], HB.epoch, timeSeries.length);
            }).collect(Collectors.toList()));

            CorrBoundTuple boundTuple = HB.CB.calcBound(newLSH, newRSH);

//            Group new DCC
            DCC newDCC = new DCC(newLSH, newRSH, boundTuple);
            HB.groupDCC(newDCC);

            if (newDCC.isPositive()){
                newPositives.add(newDCC);
            } else {
                newNegatives.add(newDCC);
            }
        });
    }

    public static PriorityQueue<Pair<Integer, TimeSeries>> scheduleArrivals(TimeSeries[] timeSeries){
        int estArrivals = Arrays.stream(timeSeries)
                .mapToInt(ts -> ts.arrivalTimes.length)
                .sum();

        PriorityQueue<Pair<Integer, TimeSeries>> queue = new PriorityQueue<>(estArrivals,
                (pairA, pairB) -> Integer.compare(pairA.x, pairB.x));

        Arrays.stream(timeSeries).forEach(ts -> Arrays.stream(ts.arrivalTimes).forEach(t -> queue.add(new Pair<>(t, ts))));

        return queue;
    }


    private void saveStats(){
        String dataFile = (String) parameters.get("dataFile");
        int p = (int) parameters.get("pLeft") + (int) parameters.get("pRight");
//        Write output to file
        try{
            String rootdirName =  "output/" + parameters.get("codeVersion") + "/" + dataFile + "/p" + p;
            File file = new File(rootdirName);
            boolean dirCreated = file.mkdir();

            String nDirName = rootdirName + "/n" + HB.n_vec;
            file = new File(nDirName);
            dirCreated = file.mkdir();

            String algorithm = (String) parameters.get("algorithm");
            int run = (int) parameters.get("run");

            String dirname = nDirName + "/" +
                    algorithm +
                    "_n" + HB.n_vec +
                    "_w" + timeSeries[0].w +
                    "_tau" + HB.tau + "_minJump" + HB.minJump;
            file = new File(dirname);
            dirCreated = file.mkdir();

            System.out.println("saving to " + dirname);

            FileWriter resultWriter = new FileWriter(dirname + "/results" + (run > 1 ? run:  "") + ".csv", false);
            resultWriter.write("timestamp,simtime,violationCount,avgGroupingSize,falseViolations,duration,arrivalCount," +
                    "dccCount,avgDccSize,clusterDiff\n");
            for (int i = 0; i < timestampArray.size(); i++) {
                double timestamp = timestampArray.size() >= i ? timestampArray.get(i): 0;
                double ST = simTimes.size() > i? simTimes.get(i): 0;
                int VC = violationCounts.size() > i ? violationCounts.get(i): 0;
                double AGS = avgGroupingSize.size() > i ? avgGroupingSize.get(i): 0;
                int FV = falseViolations.size() > i ? falseViolations.get(i): 0;
                double DA = durationArray.size() > i ? durationArray.get(i): 0;
                int AC = arrivalCount.size() > i ? arrivalCount.get(i): 0;
                int DC = dccCounts.size() > i ? dccCounts.get(i): 0;
                int DS = dccSizes.size() > i ? dccSizes.get(i): 0;
                double CD = clusterDiff.size() > i ? clusterDiff.get(i): 0;


                resultWriter.write(timestamp + "," + ST + "," + VC + "," + AGS
                        + "," + FV + "," + DA + "," + AC + "," +
                        DC + "," + (DS / DC) + "," + CD + "\n");
            }
            resultWriter.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
