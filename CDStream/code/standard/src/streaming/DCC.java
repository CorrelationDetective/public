package streaming;

import _aux.Pair;
import clustering.Cluster;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class DCC extends Object {
    public ArrayList<Cluster> LHS;
    public ArrayList<Cluster> RHS;
    public CorrBoundTuple boundTuple;
    public int lastUpdateTime;
    public boolean dead;
    public boolean WIP;


    public DCC(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, CorrBoundTuple boundTuple) {
        this.LHS = LHS;
        this.RHS = RHS;
        this.boundTuple = boundTuple;
        this.dead = false;
        this.lastUpdateTime = 0;

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
}