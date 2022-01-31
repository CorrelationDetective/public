package streaming;

import _aux.Key;
import _aux.lib;
import bounding.CorrelationBounding;
import clustering.Cluster;
import com.sun.istack.internal.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DCC implements Comparable {
    public ArrayList<Cluster> LHS;
    public ArrayList<Cluster> RHS;
    public CorrBoundTuple boundTuple;
    public int lastUpdateTime;
    public boolean dead;
    public boolean WIP;
    public DCC parent;
    public BitSet[] lhsBitMap;
    public BitSet[] rhsBitMap;

    public DCC(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, CorrBoundTuple boundTuple) {
        this.LHS = LHS;
        this.RHS = RHS;
        this.boundTuple = boundTuple;
        this.dead = false;
        this.lastUpdateTime = 0;

        int n = LHS.get(0).bitMap.size();

        this.lhsBitMap = LHS.stream().map(Cluster::getBitMap).toArray(BitSet[]::new);
        this.rhsBitMap = RHS.stream().map(Cluster::getBitMap).toArray(BitSet[]::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key)) return false;
        DCC dcc = (DCC) o;
        return this.getContent(true).equals(dcc.getContent(true));
    }

    @Override
    public int compareTo(@NotNull Object o) {
        DCC other = (DCC) o;
        return Double.compare(this.boundTuple.lower, other.boundTuple.lower);
    }

    public List<Integer> getContent(boolean sort){
        ArrayList<Cluster> clusters = this.getClusters();
        if (sort){
            return clusters.stream().flatMap(c -> c.listOfContents.stream()).sorted().collect(Collectors.toList());
        }else {
            return clusters.stream().flatMap(c -> c.listOfContents.stream()).collect(Collectors.toList());
        }
    }




    public ArrayList<Cluster> getClusters(){
        ArrayList<Cluster> out = new ArrayList<>();
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

    public boolean isSubsetOf(DCC other){
        boolean lhsResult = IntStream.range(0, lhsBitMap.length)
                .allMatch(i -> lib.bitSetIsSubsetOf(lhsBitMap[i], other.lhsBitMap[i]));
        boolean rhsResult = IntStream.range(0, rhsBitMap.length)
                .allMatch(i -> lib.bitSetIsSubsetOf(rhsBitMap[i], other.rhsBitMap[i]));
        return lhsResult && rhsResult;
    }

    public DCC splitOff(DCC other, TimeSeries[] timeSeries, int t, CorrelationBounding CB){
        ArrayList<Cluster> newLSH = new ArrayList<>();
        ArrayList<Cluster> newRSH = new ArrayList<>();

        for (int i=0; i<this.LHS.size(); i++){
            Cluster thisCluster = this.LHS.get(i);
            Cluster otherCluster = other.LHS.get(i);

            if (thisCluster.listOfContents.size() == otherCluster.listOfContents.size()){
                newLSH.add(thisCluster);
            } else {
                Cluster newCluster = thisCluster.splitOff(otherCluster, timeSeries, t);
                newLSH.add(newCluster);
            }

        }

        for (int i=0; i<this.RHS.size(); i++){
            Cluster thisCluster = this.RHS.get(i);
            Cluster otherCluster = other.RHS.get(i);

            if (thisCluster.listOfContents.size() == otherCluster.listOfContents.size()){
                newRSH.add(thisCluster);
            } else {
                Cluster newCluster = thisCluster.splitOff(otherCluster, timeSeries, t);
                newRSH.add(newCluster);
            }
        }

        CorrBoundTuple boundTuple = CB.calcBound(newLSH, newRSH);
        return new DCC(newLSH, newRSH, boundTuple);
    }
}