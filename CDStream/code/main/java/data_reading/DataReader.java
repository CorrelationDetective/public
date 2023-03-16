package data_reading;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataReader {

//    TODO MAKE THIS ADAPTIVE -- AUTOMATICALLY DETECT COLUMN/ROW MAJOR DATA
    public static Pair<String[], double[][]> readColumnMajorCSV(String path) {
        String delimiter = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));

    //            Get Header
            String firstLine = br.readLine();
            String[] header = firstLine.split(delimiter);
            int maxN = header.length;

    //            Parse data
            ArrayList<ArrayList<Double>> rows = new ArrayList<>(maxN);
            for (int i = 0; i < maxN; i++) {
                rows.add(new ArrayList<>());
            }

            while (br.ready()) {
                String[] line = br.readLine().split(delimiter);
    //                Distribute values over columns
                for (int i = 0; i < maxN; i++) {
                    if (line[i].equals("nan")) {
                        System.out.println("nan value");
                    }
                    rows.get(i).add(Double.parseDouble(line[i]));
                }
            }

    //            Remove the rows that have too low variance (if skipvar on)
            double[][] goodRows = rows.stream()
                    .map(row -> row.stream().mapToDouble(Double::doubleValue).toArray())
                    .filter(arrRow -> lib.std(arrRow) >= 1e-3 && lib.max(arrRow) - lib.min(arrRow) > 0)
                    .collect(Collectors.toList()).toArray(new double[0][]);

            return new Pair<>(header, goodRows);
        } catch (Exception e) {
            System.err.println("Error when reading data!");
            e.printStackTrace();
        }
        return null;
    }
}

