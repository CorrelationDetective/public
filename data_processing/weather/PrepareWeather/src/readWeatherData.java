import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.tools.tar.*;

public class readWeatherData {
    public static boolean excludeFileName(String filename) {
        return filename.hashCode()%fileSelectionProbability!=0;
    }
    static int fileSelectionProbability = 4000;
    static int discretization=200;

    public static double[][] readWeather(String filename, int numItems, int numRounds, Measure.Attribute attr) {
        HashMap<String,ArrayList<Measure>> readback = new HashMap<>();
        try {
            BufferedReader br2=null;
            if (filename.contains(".gz")) {
                InputStream fileStream = new FileInputStream(filename);
                InputStream gzipStream = new GZIPInputStream(fileStream);
                Reader decoder = new InputStreamReader(gzipStream);
                br2 = new BufferedReader(decoder);
            } else {
                br2= new BufferedReader(new FileReader(filename));
            }

            int notNullItems = Integer.parseInt(br2.readLine());
            int dimensions =Integer.parseInt(br2.readLine());
            int discretization = Integer.parseInt(br2.readLine());
            if (discretization<numRounds||numItems>notNullItems) return null;
            String id = "";
            while (br2.ready()) {
                id = (br2.readLine());
                ArrayList<Measure> measures = new ArrayList<>();
                for (int cnt=0;cnt<discretization;cnt++) {
                    measures.add(Measure.fromString2(id, br2.readLine()));
                }
                readback.put(id, measures);
            }
            double[][]ret = new double[numItems][];
            int cnt=0;
            for (Map.Entry<String,ArrayList<Measure>> e:readback.entrySet()) {
                int pos=0;
                double[] dimVals = new double[numRounds];
                for (Measure m:e.getValue()) {
                    dimVals[pos++] = m.getAttribute(attr);
                    if (pos==numRounds) break;
                }
                dimVals = lib.znorm(dimVals);
                ret[cnt]=dimVals;
                cnt++;
                if (cnt==numItems) break;
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void increaseCount(HashMap<String, Integer> map, String key, int count) {
        Integer res = map.get(key);
        if (res == null)
            map.put(key, count);
        else
            map.put(key, res + count);
    }

    public static int getCount(HashMap<String, Integer> map, String key) {
        Integer res = map.get(key);
        if (res == null)
            return 0;
        else
            return res;
    }
    public static void addToList(HashMap<String, ArrayList<String>> ii, String key, String val) {
        ArrayList<String> al = ii.get(key);
        if (al == null) {
            al = new ArrayList<>();
            ii.put(key, al);
        }
        al.add(val);
    }

    public static float FahrToCelcius(float fahr) {
        return 5f/9f*(fahr-32f);
    }
    public static double CelciusToKelvin(double celcius) {
        return celcius+273.15;
    }

}

