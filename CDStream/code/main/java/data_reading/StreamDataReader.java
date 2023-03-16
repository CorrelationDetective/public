package data_reading;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastLinkedList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamDataReader {

public static HashMap<String, FastLinkedList<Double[]>> getArrivalPackages(String dirPath, Integer maxT, boolean timeBased) {
    String delimiter = ",";
    try {
        HashMap<String, FastLinkedList<Double[]>> result = new HashMap<>();

//        Return empty result if no data
        if (dirPath == null) return null;

        List<String> files = Files.list(new File(dirPath).toPath())
                .map(x -> x.toString())
                .collect(Collectors.toList());

        for (String path : files){
            BufferedReader br = new BufferedReader(new FileReader(path));

            path = path.replaceAll("\\\\", "/");
            String[] pathElements = path.split("/");
            String key = pathElements[pathElements.length-1].replaceAll(".csv", "");

            String[] header = br.readLine().split(delimiter);

//            Parse data
            FastLinkedList<Double[]> arrivals = new FastLinkedList<>();

            int i=0;
            while (br.ready()) {
                String[] line = br.readLine().split(delimiter);
                Double t = Double.parseDouble(line[0]);

//                Break if enough arrivals
                if ((timeBased && t>maxT) || (!timeBased && i>maxT)){break;}

                Double val = Double.parseDouble(line[1]);
                arrivals.add(new Double[]{t, val});
                i++;
            }
//                Add to result
            result.put(key, arrivals);
        }

        return result;

    } catch (Exception e) {
        e.printStackTrace();
    }
    return null;
}
}
