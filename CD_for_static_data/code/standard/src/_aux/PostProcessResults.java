package _aux;

import bounding.CorrelationBounding;
import clustering.Cluster;
import clustering.ClusterCombination;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static _aux.lib.getStream;

public class PostProcessResults {


    public static List<String> removeDuplicatesAndToString(List<ClusterCombination> positiveDCCs, List<String> headerWithvectorNames, CorrelationBounding CB, boolean parallel){
        List<String>header = headerWithvectorNames;


        List<String> subsets = getStream(positiveDCCs, parallel)
                .map(cc -> {
                    ArrayList<Cluster> l = cc.getClusters();
                    ArrayList<String> out = new ArrayList<>();
                    boolean duplicate = (l.get(0) == l.get(1)) && (l.size() == 2); // remove multipoles of size 2 that are identical vectors

                    if (cc.isMultipoleCandidate()) { // just get the points and the corresponding names from header
                        for (Cluster c : l) {
                            //                            if (c.listOfContents.size() > 1) {
                            //                                System.err.println("more than one point in cluster!");
                            //                            }
                            int pID = c.listOfContents.get(0);
                            String s = header.get(pID);
                            out.add(s + ", ");
                            //                        String s = Integer.toString(pID);
                            //                        out.add("point" + s);
                            Collections.sort(out);
                        }


                    } else { // also make distinction for left and right hand side
                        ArrayList<Cluster> LHS = cc.getLHS();
                        ArrayList<Cluster> RHS = cc.getRHS();

                        out.add("(");

                        for (Cluster c : LHS) {
                            //                            if (c.listOfContents.size() > 1) {
                            //                                System.err.println("more than one point in cluster!");
                            //                            }
                            int pID = c.listOfContents.get(0);
                            String s = header.get(pID);
                            out.add(s);
                            out.add(", ");
                            //                        String s = Integer.toString(pID);
                            //                        out.add("point" + s);
                        }
                        out.add("), (");

                        for (Cluster c : RHS) {
                            //                            if (c.listOfContents.size() > 1) {
                            //                                System.err.println("more than one point in cluster!");
                            //                            }
                            int pID = c.listOfContents.get(0);
                            String s = header.get(pID);
                            out.add(s);
                            out.add(", ");
                            //                        String s = Integer.toString(pID);
                            //                        out.add("point" + s);
                        }
                        out.add(")");


                    }

//                    CB.calcBound(cc, true, false);

//                    Collections.sort(out);

                    out.add(" \t \t \t \t \t corr: " + (double) Math.round(cc.getLB() * 1000) / 1000 + ". Max subset corr: " + (double) Math.round(cc.getMaxSubsetCorr(CB) * 1000) / 1000);

                    //                    if(cc.getClusters().size()>2){
                    //                        System.out.println("check this");
                    //                    }

                    String stringOut = "";
                    for (String s : out) {
                        stringOut = stringOut + s;
                    }
                    if (!duplicate) {
                        return stringOut;
                    } else {
                        return null;
                    }

                })
                .filter(Objects::nonNull)
                .sorted()
//                .distinct()
                .collect(Collectors.toList());
        return subsets;




    }

    public static List<ClusterCombination> unpackAndCheckMinJump(List<ClusterCombination> positiveDCCs, CorrelationBounding CB, double minJump, boolean parallel){
        if(CB == null){
            System.err.println("pardon?");
        }

        List<ClusterCombination> out;

        out = getStream(positiveDCCs, parallel).unordered()
                .flatMap(cc -> cc.getSingletonClusters().stream())
                .filter(cc -> { // remove cases where LHS and RHS overlap
//                    return !(cc.getClusters().size() == 2 && cc.getClusters().get(0) == cc.getClusters().get(1));

                    if(cc.isMultipoleCandidate()){
                        return true;
                    }else{
                        for(Cluster c : cc.getLHS()){
                            if(cc.getRHS().contains(c)){
                                return false;
                            }
                        }
                        return true;
                    }
                })
                .filter(cc -> {



                    CB.calcBound(cc, true);
                    if (Math.abs(cc.getLB() - cc.getUB()) > 0.001) {
                        System.err.println("postprocessing: not a singleton comparison");
                    }
                    return (cc.getMaxSubsetCorr(CB) + minJump) < cc.getLB();
                })
                .collect(Collectors.toList());
        return out;

    }

    public static void printResultSet(List<ClusterCombination> positiveDCCs, CorrelationBounding CB, double minJump, List<String> header, boolean parallel){
        List<ClusterCombination> ccset = unpackAndCheckMinJump(positiveDCCs, CB, minJump, parallel);
        List<String> stringset = removeDuplicatesAndToString(ccset, header, CB, parallel);

        try {
            TimeUnit.MILLISECONDS.sleep(10); // to prevent sometimes shuffled output
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println();

        System.out.println("Total number of positives: " + stringset.size());


        System.out.println("Interesting subsets:");
        System.out.println();

        stringset.sort(Comparator.comparingInt(l -> l.split(",").length));



        for(String s : stringset){
            System.out.println(s);
        }
    }

}