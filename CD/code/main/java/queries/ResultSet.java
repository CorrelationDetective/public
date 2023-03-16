package queries;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import bounding.ClusterCombination;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.*;
import java.util.stream.Collectors;

// Class that holds the (running) results of a query as a set of (positive) ClusterCombinations
public class ResultSet {
    //    In case topk- this holds at most k results in a MinHeap
    @Getter @Setter
    private Queue<ResultObject> results;

    @NonNull
    public final int topK;

    @NonNull
    public final boolean irreducibility;

    @NonNull
    public final QueryTypeEnum queryType;

//    For saving results of topk queries

    @NonNull
    private final boolean saveHistory;

    @Getter
    private final LinkedList<ResultObject> resultHistory = new LinkedList<>();


    public final static int MAX_RESULTS = 1000000;

//--------------------------------------------------------------

    public ResultSet(QueryTypeEnum queryType, int topK, boolean irreducibility, boolean saveHistory){
        this.topK = topK;
        this.queryType = queryType;
        this.irreducibility = irreducibility;
        this.saveHistory = saveHistory && queryType == QueryTypeEnum.TOPK;

        if (queryType == QueryTypeEnum.TOPK) {
            results = new PriorityQueue<>(2*topK, Comparator.comparing(ResultObject::getSimilarity));
        } else {
            results = new LinkedList<>();
        }
    }

    public void add(ResultObject resultObject, RunningThreshold runningThreshold) throws ProgressiveStopException {
        FastLinkedList<ResultObject> results = new FastLinkedList<>();
        results.add(resultObject);
        this.addAll(results, runningThreshold);
    }

    public void addAll(FastLinkedList<ResultObject> newResults, RunningThreshold runningThreshold) throws ProgressiveStopException {
        switch (queryType) {
            case TOPK: {
                for (ResultObject res : newResults) {
                    double sim = res.getSimilarity();

//                Add to topk (if still necessary)
                    synchronized (results) {
                        if (sim > runningThreshold.get()) {
                            results.add(res);

//                            Add to result history
                            if (saveHistory){
                                synchronized (resultHistory) {
                                    resultHistory.add(res);
                                }
                            }

//                            Get rid of worst result and update threshold
                            if (results.size() > topK) {
                                results.poll();
                                runningThreshold.setThreshold(results.peek().getSimilarity());
                            }
                        }
                    }
                }

                break;
            }
            case PROGRESSIVE: {
                synchronized (results) {
                    if (newResults.size() + results.size() > topK) {
                        for (int i = 0; i < topK - results.size(); i++) {
                            this.results.add(newResults.removeFirst());
                        }
                        throw new ProgressiveStopException("Early stopping - required results reached");
                    } else {
                        this.results.addAll(newResults.toList());
                    }
                }
                break;
            }
            case THRESHOLD: {
                synchronized (results) {
//                    Remove some results first if needed
                    if (results.size() + newResults.size() > MAX_RESULTS) {
                        for (int i = 0; i < newResults.size(); i++) {
                            results.poll();
                        }
                    }
                    this.results.addAll(newResults.toList());
                }
                break;
            }
        }
    }

    public FastArrayList<ResultTuple> toResultTuples(Queue<ResultObject> results, String[] headers){
        FastArrayList<ResultTuple> out = new FastArrayList<>(results.size() + 1);
        for (ResultObject res: results){
            if (res instanceof ResultTuple){
                out.add((ResultTuple) res);
            } else if (res instanceof ClusterCombination){
                out.add(((ClusterCombination) res).toResultTuple(headers));
            }
        }

        return out;
    }

    public int size() {
        return results.size();
    }
}
