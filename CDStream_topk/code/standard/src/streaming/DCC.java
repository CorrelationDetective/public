package streaming;

import _aux.Key;
import bounding.CorrelationBounding;
import clustering.Cluster;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

public class DCC implements Comparable {
    public ArrayList<Cluster> LHS;
    public ArrayList<Cluster> RHS;
    public CorrBoundTuple boundTuple;
    public int lastUpdateTime;
    public boolean dead;
    public boolean WIP;
    public DCC parent;
    public int groupedState;


    public DCC(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, CorrBoundTuple boundTuple) {
        this.LHS = LHS;
        this.RHS = RHS;
        this.boundTuple = boundTuple;
        this.dead = false;
        this.lastUpdateTime = 0;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key)) return false;
        DCC dcc = (DCC) o;
        return this.getContent().equals(dcc.getContent());
    }

    @Override
    public int compareTo(Object o) {
        DCC other = (DCC) o;
        return Double.compare(this.boundTuple.lower, other.boundTuple.lower);
    }

    public List<Integer> getContent(){
        ArrayList<Cluster> clusters = this.getClusters();
        return clusters.stream().flatMap(c -> c.listOfContents.stream()).sorted().collect(Collectors.toList());
    }

    public ArrayList<Cluster> getClusters(){
        ArrayList<Cluster> out = new ArrayList<>(LHS.size() + RHS.size());
        out.addAll(LHS); out.addAll(RHS);
        return out;
    }

    public boolean isPositive(){
        return boundTuple.state == 1;
    }

    public boolean isDead(){
        return dead;
    }

    public boolean isAlive(){
        return !dead;
    }

    public String toString(){
        return getClusters().stream().map(Cluster::getClusterId).collect(Collectors.toList()).toString();
    }

    public DCC clone(){
        return new DCC(this.LHS, this.RHS, this.boundTuple);
    }

    public int getSize(){
        return getClusters().stream().mapToInt(c -> c.listOfContents.size()).sum();
    }

    public boolean isSingleton(){
        return LHS.stream().allMatch(c -> c.listOfContents.size() == 1) && RHS.stream().allMatch(c -> c.listOfContents.size() == 1);
    }

    public List<DCC> unpackToSingletons(TimeSeries[] timeSeries, CorrelationBounding CB){
//        If already singleton, skip
        if (this.isSingleton()){
            this.parent = this;
            return Collections.singletonList(this);
        }


        List<List<Cluster>> lhsSingletons = this.LHS.stream().map(c -> c.getSingletons(timeSeries)).collect(Collectors.toList());
        List<List<Cluster>> rhsSingletons = this.RHS.stream().map(c -> c.getSingletons(timeSeries)).collect(Collectors.toList());


        List<List<Cluster>> allSingletons = new ArrayList<>();
        allSingletons.addAll(lhsSingletons);
        allSingletons.addAll(rhsSingletons);

        List<List<Cluster>> cartesianProduct = Lists.cartesianProduct(allSingletons).stream()
                .map(x -> x.stream().collect(Collectors.toList())).collect(Collectors.toList());

        List<DCC> singletonDCC = cartesianProduct.stream()
                .map(cc -> {
                    ArrayList<Cluster> newLHS = new ArrayList<>(cc.subList(0, LHS.size()));
                    ArrayList<Cluster> newRHS = new ArrayList<>(cc.subList(LHS.size(), cc.size()));

                    CorrBoundTuple corrBoundTuple = CB.calcBound(newLHS, newRHS, 1d, true);
                    DCC newDCC = new DCC(newLHS, newRHS, corrBoundTuple);
                    newDCC.parent = this;
                    return newDCC;
                }).collect(Collectors.toList());

        return singletonDCC;
    }


}