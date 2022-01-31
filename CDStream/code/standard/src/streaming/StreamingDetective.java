package streaming;

import _aux.Key;
import _aux.Pair;
import _aux.Tuple3;
import _aux.lib;
import bounding.HierarchicalBounding;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    public String batchModel;
    public int batchSize;

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
    public ArrayList<Double> epochSizes;
    public ArrayList<Integer> simTimes;

    HashMap<String, Object> parameters;


    public StreamingDetective(HashMap<String, Object> parameters){
        this.parameters = parameters;

        this.timeSeries = (TimeSeries[]) parameters.get("timeSeries");
        this.HB = (HierarchicalBounding) parameters.get("HB");

        this.batchModel = (String) parameters.get("batchModel");
        this.epochSize = (int) parameters.get("epochSize");
        this.batchSize = (int) parameters.get("batchSize");

        this.saveStats = (boolean) parameters.get("saveStats");

        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        this.jvmStartTime = bean.getStartTime();
        this.cores = Runtime.getRuntime().availableProcessors();
    }

    public ArrayList<Double> simulate() {
        int maxT = (int) parameters.get("maxT");
        this.parallel = (boolean) parameters.get("parallel");
        this.violationCounts = new ArrayList<>(maxT / (int) epochSize);
        this.avgGroupingSize = new ArrayList<>(maxT / (int) epochSize);
        this.arrivalCount = new ArrayList<>(maxT / (int) epochSize);
        this.durationArray = new ArrayList<>(maxT / (int) epochSize);
        this.dccCounts = new ArrayList<>(maxT / (int) epochSize);
        this.dccSizes = new ArrayList<>(maxT / (int) epochSize);
        this.timestampArray = new ArrayList<>(maxT / (int) epochSize);
        this.clusterDiff = new ArrayList<>(maxT / (int) epochSize);
        this.falseViolations = new ArrayList<>(maxT / (int) epochSize);
        this.epochSizes = new ArrayList<>(maxT / (int) epochSize);
        this.simTimes = new ArrayList<>(maxT / (int) epochSize);

        ArrayList<Double> durations = new ArrayList<>();

        this.seed = (int) parameters.get("seed");

//        Schedule all epochs before simulating
        this.arrivalQueue = scheduleArrivals(timeSeries);

//        Simulate
        while (this.t < maxT) {
            HB.epoch++;
            this.t += epochSize;
            this.simTimes.add(this.t);

//            Get arrivals
            List<TimeSeries> arrivals = new ArrayList<>();
            Pair<Integer, TimeSeries> queuePointer = arrivalQueue.peek();

            while (queuePointer != null &&
                    ((batchModel.equals("timeBased") && queuePointer.x <= this.t) ||
                    (batchModel.equals("arrivalBased") && arrivals.size() < batchSize))
            ) {
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

            epochSizes.add(epochSize);

            long cycleStart = System.currentTimeMillis();
            timestampArray.add(StreamLib.getDuration(jvmStartTime, System.currentTimeMillis()));

            double dur;

            if (arrivals.size() > 0){
                arrival(new HashSet<>(arrivals));
                dur = StreamLib.getDuration(cycleStart, System.currentTimeMillis());

            } else {
                dur = 0;
                arrivalCount.add(0);
            }

            durations.add(dur);
            durationArray.add(dur);

            dccCounts.add(HB.dccCount);
            dccSizes.add(HB.totalDCCSize);
        }

        this.saveStats();

        return durations;
    }

    public void arrival(Set<TimeSeries> arrivals){
        if (saveStats){arrivalCount.add(arrivals.size());}

//        Set old correlations
        lib.getStream(timeSeries, parallel).forEach(stock -> stock.oldCorrelations = stock.pairwiseCorrelations.clone());

//        Update correlation cache for that stock
        lib.getStream(arrivals, parallel).forEach(stock -> stock.computePairwiseCorrelations(timeSeries, true));

        String algorithm = (String) parameters.get("algorithm");


        if (!algorithm.equals("oneshot")){
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

        } else {
            parameters.put("t", HB.epoch);
            HierarchicalBounding HB = new HierarchicalBounding(parameters);
            this.HB = HB;
            Set<DCC> resultSet = HB.recursiveBounding(parameters);
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

    private void handleLevel2Violation(DCC dcc) throws IllegalStateException {
//        Check again if corrbound is violated, could have been updated in the meantime
        if (dcc.dead || dcc.lastUpdateTime == HB.epoch){
            return;
        }

        int oldState = dcc.boundTuple.state;

//            Compute new bound after violation
        dcc.boundTuple = HB.CB.calcBound(dcc.LHS, dcc.RHS);

        int newState = dcc.boundTuple.state;
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
                    HB.resultSet.remove(dcc);
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
                HB.resultSet.addAll(HB.breakUCC(dcc.LHS, dcc.RHS));

            } else if (newState != oldState){

//            Pos to neg
                if (newState == -1){
                    HB.resultSet.remove(dcc);
                } else {
//                Neg to pos
                    HB.resultSet.add(dcc);
                }

//                Group again
                HB.groupDCC(dcc);
            }
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
        int p = (int) parameters.get("pLeft") + (int) parameters.get("pRight");
//        Write output to file
        try{
            String rootdirName =  "output/" + parameters.get("codeVersion") + "/p" + p;
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
                    "_tau" + HB.tau + "_minJump" + HB.minJump +
                    "_batchSize" + batchSize;
            file = new File(dirname);
            dirCreated = file.mkdir();

            System.out.println("saving to " + dirname);

            FileWriter resultWriter = new FileWriter(dirname + "/results" + (run > 1 ? run:  "") + ".csv", false);
            resultWriter.write("timestamp,simtime,epochSize,violationCount,avgGroupingSize,falseViolations,duration,arrivalCount," +
                    "dccCount,avgDccSize,clusterDiff\n");
            for (int i = 0; i < timestampArray.size(); i++) {
                double timestamp = timestampArray.size() >= i ? timestampArray.get(i): 0;
                double ST = simTimes.size() > i? simTimes.get(i): 0;
                double ES = epochSizes.size() > i? epochSizes.get(i): 0;
                int VC = violationCounts.size() > i ? violationCounts.get(i): 0;
                double AGS = avgGroupingSize.size() > i ? avgGroupingSize.get(i): 0;
                int FV = falseViolations.size() > i ? falseViolations.get(i): 0;
                double DA = durationArray.size() > i ? durationArray.get(i): 0;
                int AC = arrivalCount.size() > i ? arrivalCount.get(i): 0;
                int DC = dccCounts.size() > i ? dccCounts.get(i): 0;
                int DS = dccSizes.size() > i ? dccSizes.get(i): 0;
                double CD = clusterDiff.size() > i ? clusterDiff.get(i): 0;


                resultWriter.write(timestamp + "," + ST + "," + ES + "," + VC + "," + AGS
                        + "," + FV + "," + DA + "," + AC + "," +
                        DC + "," + (DS / DC) + "," + CD + "\n");
            }
            resultWriter.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
