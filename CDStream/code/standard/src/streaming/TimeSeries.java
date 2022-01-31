package streaming;


import _aux.lib;

import java.util.*;


public class TimeSeries {
    public final double[] data;
    public final int[] arrivalTimes;
    public double[] slidingData;
    public double[] pairwiseCorrelations;
    public double[] oldCorrelations;
    public final int w;
    public int updateCount;
    public final int id;
    public final String name;


    public TimeSeries(int id, String name, double[] data, int[] arrivalTimes, int w, int n){
        this.id = id;
        this.name = name;
        this.data = data;
        this.arrivalTimes = arrivalTimes;
        this.updateCount = 0;
        this.w = w;
        this.slidingData = lib.znorm(Arrays.copyOfRange(data, 0, w));

        this.pairwiseCorrelations = new double[n];
    }

    public String toString(){return String.valueOf(this.id);}

    public void progressWindow(int t) throws IllegalStateException{
        this.updateCount++;

        if (w + t > data.length){
            throw new IllegalStateException("No more data left for stock! Stock has had " + updateCount + " updates");
        }
        this.slidingData = lib.znorm(Arrays.copyOfRange(data, t, w + t));
    }

    public void computePairwiseCorrelations(TimeSeries[] timeSeries, boolean all){
        this.oldCorrelations = this.pairwiseCorrelations.clone();

        this.pairwiseCorrelations[this.id] = 1d;
        for (int i = all ? 0 : this.id + 1; i< timeSeries.length; i++){
            if (i == id){
                continue;
            }
            this.pairwiseCorrelations[i] = Math.max(Math.min(lib.pearsonWithAlreadyNormedVectors(timeSeries[i].slidingData, this.slidingData), 1), -1);

            timeSeries[i].pairwiseCorrelations[this.id] = this.pairwiseCorrelations[i];
        }
    }

    public void setSlidingData(int t){
        this.updateCount = t;
        this.slidingData = lib.znorm(Arrays.copyOfRange(data, t, w + t));
    }


//    TODO WAS PART OF VARIABLE BINNING
//    public void initializeGroupingFromTmp(boolean parallel, int nrBins){
//        ubNodes = new HashMap<>();
//        lbNodes = new HashMap<>();
//
//        lib.getStream(ubNodes.keySet(), parallel).forEach(key -> {
//            List<Pair<Double, DCC>> tmpNode = ubNodes.get(key);
//            TreeMap<Double, List<DCC>> binnedNode = binDccList(tmpNode, true, nrBins, parallel);
//
//            ubNodes.put(key, binnedNode);
//        });
//        lib.getStream(lbTmp.keySet(), parallel).forEach(key -> {
//            List<Pair<Double, DCC>> tmpNode = lbTmp.get(key);
//            TreeMap<Double, List<DCC>> binnedNode = binDccList(tmpNode, false, nrBins, parallel);
//
//            lbNodes.put(key, binnedNode);
//        });
//
//        ubTmp = null;
//        lbTmp = null;
//    }
//
//    public TreeMap<Double, List<DCC>> binDccList(List<Pair<Double, DCC>> tmpNode, boolean ub, int nrBins, boolean parallel){
//        //            Handle positive nodes
//        double start = tmpNode.stream().mapToDouble(pair -> pair.x).min().getAsDouble();
//        double end = tmpNode.stream().mapToDouble(pair -> pair.x).max().getAsDouble();
//
//        TreeMap<Double, List<DCC>> binnedNode = ub ? new TreeMap<>(): new TreeMap<>(Collections.reverseOrder());
//
////        All DCCs have the same threshold -> make only one group
//        if (start == end){
//            binnedNode.put(start, tmpNode.stream().map(pair -> pair.y).collect(Collectors.toList()));
//        } else {
//            lib.getStream(tmpNode, parallel).forEach(pair -> {
//                double bin = StreamLib.getBin(start, end, nrBins, pair.x);
//                binnedNode.merge(bin, new ArrayList<>(Arrays.asList(pair.y)), (prev, one) -> {prev.addAll(one); return prev;});
//            });
//        }
//
//        return binnedNode;

//    }



}
