package queries;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import bounding.ClusterCombination;
import clustering.Cluster;
import core.Parameters;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import streaming.TimeSeries;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// Class that holds the (running) results of a query as a set of (positive) ClusterCombinations
public class ResultSet {
    //    In case topk- this holds at most k results in a MinHeap
    @Getter @Setter
    private Collection<ClusterCombination> results;

    @NonNull
    public final int topK;

    @NonNull
    public final boolean irreducibility;

    @NonNull
    public final QueryTypeEnum queryType;

//    For saving results of topk queries


    public final static int MAX_RESULTS = 1000000;

//--------------------------------------------------------------

    public ResultSet(QueryTypeEnum queryType, int topK, boolean irreducibility) {
        this.topK = topK;
        this.queryType = queryType;
        this.irreducibility = irreducibility;

        if (queryType == QueryTypeEnum.TOPK) {
            results = new PriorityQueue<>(2*topK, Comparator.comparing(ClusterCombination::getSimilarity));
        } else {
            results = new HashSet<>();
        }
    }

    public ResultSet clone(){
        ResultSet clone = new ResultSet(queryType, topK, irreducibility);
        clone.results = queryType == QueryTypeEnum.TOPK ? new PriorityQueue<>(results): new HashSet<>(results);
        return clone;
    }

    public int size() {
        return results.size();
    }

    public void clear() {
        results.clear();
    }

    public ClusterCombination add(ClusterCombination cc, RunningThreshold runningThreshold) throws ProgressiveStopException {
//        Check if not already in resultSet, if so, ignore dcc (make sure not to index by killing it)
        if (results.contains(cc)) {cc.kill(); return null;}

        switch (queryType) {
            case TOPK: {
                PriorityQueue<ClusterCombination> results = (PriorityQueue<ClusterCombination>) this.results;
                double sim = cc.getLB();

//                Add to buffer (if still in topK)
                synchronized (results) {
                    if (sim > runningThreshold.get()) {
                        results.add(cc);

//                            Get rid of worst result and update threshold
                        if (results.size() > topK) {
                            ClusterCombination worst = results.poll();
                            worst.setPositive(false);
                            runningThreshold.setThreshold(results.peek().getSimilarity());

//                            Return removed result
                            return worst;
                        }
                    }
                }
                break;
            }
            case PROGRESSIVE: {
                synchronized (results) {
                    results.add(cc);
                    if (results.size() > topK) {
                        throw new ProgressiveStopException("Early stopping - required results reached");
                    }
                }
                break;
            }
            case THRESHOLD: {
                HashSet<ClusterCombination> results = (HashSet<ClusterCombination>) this.results;
                synchronized (results) {
//                    Remove some results first if needed
                    if (results.size() + 1 > MAX_RESULTS) {
                        results.clear();
                    }
                    this.results.add(cc);
                }
                break;
            }
        }
        return null;
    }

    public void remove(ClusterCombination ClusterCombination) {
        results.remove(ClusterCombination);
    }

    public FastArrayList<ResultTuple> toResultTuples(TimeSeries[] timeSeries){
        FastArrayList<ResultTuple> out = new FastArrayList<>(results.size() + 1);
        out.addAll(results.stream().map(cc -> cc.toResultTuple(timeSeries)).collect(Collectors.toList()));
        return out;
    }



    public void writeOut(FileWriter writer, int epoch, TimeSeries[] timeSeries){
        try {
            FastArrayList<ResultTuple> resultTuples = this.toResultTuples(timeSeries);

//            Write results
            for (ResultTuple result: resultTuples){
                result.sortSides();

//                lhs,rhs,lheaders,rheaders,sim,timestamp,epoch
                writer.write(String.format("%s,%s,%s,%s,%.4f,%d,%d%n",
                        result.LHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        result.RHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        String.join("-", result.lHeaders),
                        String.join("-", result.rHeaders),
                        result.similarity,
                        result.timestamp,
                        epoch
                ));
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
