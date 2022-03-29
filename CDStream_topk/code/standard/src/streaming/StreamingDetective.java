package streaming;

import _aux.Key;
import _aux.Pair;
import _aux.Tuple3;
import _aux.lib;
import bounding.HierarchicalBounding;
import clustering.Cluster;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.BaseStream;
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

//            Update topK
            updateTopK();

        } else {
            parameters.put("t", HB.epoch);
            HierarchicalBounding HB = new HierarchicalBounding(parameters);
            this.HB = HB;
            List<DCC> resultSet = HB.recursiveBounding(parameters);
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
            ArrayList<Map<Key, List<DCC>>> grouping = ub ? HB.ubGrouping: HB.lbGrouping;

            Key idxKey = ts.id < otherId ? new Key(ts.id, otherId): new Key(otherId, ts.id);
            int groupingIndex = StreamLib.getGroupIndex(idxKey.x, idxKey.y, timeSeries.length);

            Map<Key, List<DCC>> node = grouping.get(groupingIndex);

            List<Key> removeGroups = new ArrayList<>();
            List<DCC> addLaters = new ArrayList<>();

            Set<Key> bdps = node.keySet();

//                Iterate over second level grouping
            for (Key BDP : bdps) {
                double thCorr = idxKey.equals(BDP) ? timeSeries[BDP.x].oldCorrelations[BDP.y] :
                        timeSeries[BDP.x].pairwiseCorrelations[BDP.y];

//                If newCorr exceeds threshold check stored DCCs
                if ((ub && newcorr > thCorr) || (!ub && newcorr < thCorr)) {
                    List<DCC> dccList = node.get(BDP);

                    if (dccList == null) {
                        continue;
                    }

//                    If empty group, delete from node to save time
                    if (dccList.size() == 0) {
                        removeGroups.add(BDP);
                        continue;
                    }

//                    Lazy filtering of dead DCCs
                    List<DCC> negAliveDccs = dccList.stream().filter(dcc -> dcc != null && dcc.isAlive() && dcc.boundTuple.state == -1)
                            .collect(Collectors.toList());

//                            idKey is now BDP, move later
                    if (!BDP.equals(idxKey)){
                        addLaters.addAll(negAliveDccs);

//                            Remove current node to save time later
                        removeGroups.add(BDP);
                    }
//                        Current idKey is already BDP
                    else {
                        dccList = negAliveDccs;
                    }

                    localViolations.addAll(negAliveDccs);
                }
            }
//                Add violated BDP to new BDP (which is idKey)
            node.merge(idxKey, addLaters, (old, now) -> {
                old.addAll(now);
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
        dcc.boundTuple = HB.CB.calcBound(dcc.LHS, dcc.RHS, 1d, true);

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
            if (violationCounts != null && saveStats){
                if (violationCounts.size() > HB.epoch){
                    try{
                        violationCounts.set(HB.epoch, violationCounts.get(HB.epoch) + 1);
                    } catch (NullPointerException ignored){}
                } else {
                    violationCounts.add(1);
                }
//                    violationSize[HB.t] += dcc.getSize();
            }

//            Disable DCC
                dcc.dead = true;
                HB.dccCount--;

                //            Split DCC and group
                HB.breakUCC(dcc.LHS, dcc.RHS, 1d);
            }
    }

    public void updateTopK(){
//        Cast top2kDCCs to list
        List<DCC> topkBufferedList = new ArrayList<>(HB.topkBuffered);

//        Update bound for all dccs in list
        topkBufferedList.forEach(this::handleLevel2Violation);

//        Resort top2k with insertionSort (optimal for already mainly sorted data) and partition on state (one-pass algorithm)
        Map<Boolean, List<DCC>> partitionedBuffer = lib.insertionSortAndPartition(topkBufferedList,
                DCC::compareTo, DCC::isPositive);

//        Filter out hidden negatives and add to structure
        List<DCC> hiddenNegatives = partitionedBuffer.get(false);

//        Group hidden negatives
        hiddenNegatives.forEach(dcc -> {
            if (dcc.boundTuple.state == -1){
                HB.groupDCC(dcc);
                HB.negativeDCCs.add(dcc);
            } else if (dcc.boundTuple.state == 0){
                HB.corrBounding(dcc.LHS, dcc.RHS, 1d);
            }
        });

//        Set top2kList to sorted list
        topkBufferedList = partitionedBuffer.get(true);

//        Get topK
        HB.positiveDCCs = new ArrayList<>(topkBufferedList.subList(Math.max(0, topkBufferedList.size() - HB.k), topkBufferedList.size()));

//        Cut off excess if top2kDCCs is getting too big
        if (topkBufferedList.size() > HB.topKbufferSize + HB.k){
            topkBufferedList = new ArrayList<>(topkBufferedList.subList(topkBufferedList.size() - (HB.topKbufferSize),
                    topkBufferedList.size()));
        }

//        Cut off excess if tau would suffer too much
        int begin = 0;
        while (topkBufferedList.get(begin).boundTuple.lower < HB.tau - 0.2){
            begin++;
        }
        topkBufferedList = topkBufferedList.subList(begin, topkBufferedList.size());

//        Cast top2kList back to concurrentList
        HB.topkBuffered = new ConcurrentLinkedQueue<>(topkBufferedList);

//        Update tau
        HB.tau = topkBufferedList.get(0).boundTuple.lower;
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
            int k = (int) parameters.get("k");

            String dirname = nDirName + "/" +
                    algorithm +
                    "_n" + HB.n_vec +
                    "_k" + k +
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
