package data;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

import _aux.lib;
import org.apache.tools.tar.*;
import data.Measure.Attribute;

public class readWeatherData {
    public static boolean excludeFileName(String filename) {
        return filename.hashCode()%fileSelectionProbability!=0;
    }
    static int fileSelectionProbability = 4000;
    static int discretization=200;
    public static void main(String[]args) throws IOException {
        if (args == null || args.length == 0) {

            args = new String[]{
                    "/Users/papapetrou/iCloud/datasets/noaa/", "slp",
                    "1959", "1960", "1", "200" //317565144720
            };
        }
        String filesPath = args[0];
        String attributeToExtract = args[1];
        int yearIntStart = Integer.parseInt(args[2]);
        int yearIntStop = Integer.parseInt(args[3]);
        fileSelectionProbability = Integer.parseInt(args[4]);
        discretization = Integer.parseInt(args[5]);
        long dayDuration = 24 * 60 * 60 * 1000; // one day
        long periodDurationInMillis = dayDuration;

        long maxDurationUnseen = 2*365*(yearIntStop-yearIntStart+1)/discretization*periodDurationInMillis;
          /*/
          HashMap<String, Integer> countryCount = new HashMap<>();
        HashMap<String, String> sensorCountries = new HashMap<>();
        HashMap<String, ArrayList<String>> ii = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(new File(historyPath)));
        for (int i = 0; i < 22; i++) br.readLine();
        while (br.ready()) {
            String sensorData = br.readLine();
            if (sensorData.length() < 10)
                continue;
            String sensorid = (sensorData.substring(0, 12).trim().replaceAll(" ", "-"));
            String sensorName = (sensorData.substring(13, 42).trim().replaceAll(" ", "."));
            if (sensorName.contains("BOGUS"))
                continue;
            String location = sensorData.substring(45, 48).trim(); // 42-45 the historic WMO country, 45-48 this is the second country, i.e., FIPS country id
            if (location.length() < 2)
                continue; // no location, don't keep it
            String state = sensorData.substring(49, 52).trim();
            if (state.length() < 2 && location.startsWith("US"))
                continue;
            String begin = sensorData.substring(83, 92).trim();
            String end = sensorData.substring(92).trim();
            if (begin.contains("NO") || end.contains("NO")) // starting and ending time -- NO DATA -- ignore
                continue;
            int yearBegin = Integer.parseInt(begin.substring(0, 4));
            int yearEnd = Integer.parseInt(end.substring(0, 4));
            if (yearBegin >= yearIntStart || yearEnd <= yearIntStop)
                continue;

            String finalLocation = "";
            if (location.startsWith("US"))
                finalLocation = location + " " + state;
            else
                finalLocation = location;
            increaseCount(countryCount, finalLocation, +1);
            sensorCountries.put(sensorid, finalLocation);
            addToList(ii, finalLocation.trim(), sensorid);
        }
        br.close();
        /*/
        BufferedReader br=null;
        // now load files
        HashMap<String, ArrayList<Measure>> allMeasures = new HashMap<>();
//        long expectedDiscretizationStep =dayDuration*(yearIntStop-yearIntStart+1)*365/discretization;

        HashSet<String> brokenIds = new HashSet<>();
        long fileread=0;
        HashSet<String> goodfiles = new HashSet<>();
        for (int cnt = yearIntStart; cnt <= yearIntStop; cnt++) {
            System.err.println("Year " + cnt);
            String fileName = filesPath + cnt + ".tar.gz";
            GZIPInputStream gzipInputStream = null;
            gzipInputStream = new GZIPInputStream(new FileInputStream(fileName));
            TarInputStream is = new TarInputStream(gzipInputStream);
            TarEntry entryx = null;
            SimpleDateFormat sdate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            while ((entryx = is.getNextEntry()) != null) {
                if (entryx.isDirectory()) continue;
                else {
                    boolean brokenId = false;
                    String sensorId = "-1";
                    long lastTime = 0;
                    if (excludeFileName(entryx.getName())) continue;
                    if (fileread++%1000==0)
                        System.out.println("Read " + fileread  + " files. I found " +goodfiles.size() + " good ones up to now.");

                    try {
                        if (entryx.getName().endsWith("csv")) {
                            if (yearIntStart == cnt) goodfiles.add(entryx.getName());
                            else if (!goodfiles.contains(entryx.getName()))
                                continue; // it is a new file, since i don't have for previous years i cannot use it

                            ArrayList<Measure> thisFile = new ArrayList<>();
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            BufferedOutputStream bos = new BufferedOutputStream(baos);
                            is.copyEntryContents(bos);
                            bos.flush();
                            BufferedReader breader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));

                            String line = breader.readLine();
                            int faultyLines=0;
                            while (breader.ready()) {
                                line = breader.readLine();
//                                System.err.println(line);
                                line = line.replaceAll(",,", ",\"\",");
//                                while (line.indexOf(",,") >= 0)
//                                    line = line.replaceAll(",,", ",\"\",");

                                String[] splitLine = line.split("\",\"");
                                String id = (splitLine[0].replace("\"", ""));//******
                                sensorId = id;
                                String date = splitLine[1].replace("\"", "");
                                long timestamp = 0;//******
                                try {
                                    Date d = sdate.parse(date);
                                    timestamp = d.getTime();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                if (brokenIds.contains(id)) {
                                    brokenId = true;
                                    goodfiles.remove(entryx.getName());
                                    break;
                                } else if (timestamp - maxDurationUnseen > lastTime && lastTime != 0) { // more than 2 days
//                                    Date d1 = new Date(timestamp);
//                                    Date d2 = new Date(lastTime);
//                                    System.err.println(d1 + " and " + d2);
                                    brokenId = true;
                                    goodfiles.remove(entryx.getName());
                                    brokenIds.add(id);
                                    break;
                                } else if (lastTime + periodDurationInMillis > timestamp && lastTime != 0) // not yet 1 day since last reading
                                    continue;

                                float lat=0,lon=0,elevation=0;
                                int windSpeedNot9999=0,windShouldBe1=0,visibilityNot999999=0,visibilityShouldBe1=0, tempNot9999=0,tempShouldBe1=0;
                                int dewNot9999=0,dewShouldBe1=0,slpNot99999=0,slpShouldBe1=0;
                                boolean goodline=false;
                                switch(attributeToExtract) {
                                    case "wind": {
                                        String[] windStr = splitLine[10].split(",");
                                        windSpeedNot9999 = Integer.parseInt(windStr[3]); //******
                                        windShouldBe1 = Integer.parseInt(windStr[4]); //******
                                        if (windShouldBe1==1 && windSpeedNot9999 != 9999) {
                                            goodline=true;
                                        }
                                        break;
                                    }
                                    case "visibility": {
                                        String[] visibilityStr = splitLine[12].split(",");
                                        visibilityNot999999 = Integer.parseInt(visibilityStr[0]);//******
                                        visibilityShouldBe1 = Integer.parseInt(visibilityStr[1]);//******
                                        if (visibilityShouldBe1==1 && visibilityNot999999 != 999999) {
                                            goodline=true;
                                        }
                                        break;
                                    }
                                    case "temp": {
                                        String[] tempStr = splitLine[13].split(",");
                                        tempNot9999 = Integer.parseInt(tempStr[0]);//******
                                        tempShouldBe1 = Integer.parseInt(tempStr[1]);//******
                                        if (tempShouldBe1==1 && tempNot9999 != 9999) {
                                            goodline=true;
                                        }
                                        break;
                                    }
                                    case "dew": {
                                        String[] dewStr = splitLine[14].split(",");
                                        dewNot9999 = Integer.parseInt(dewStr[0]);//******
                                        dewShouldBe1 = Integer.parseInt(dewStr[1]);//******
                                        if (dewShouldBe1==1 && dewNot9999 != 9999) {
                                            goodline=true;
                                        }
                                        break;
                                    }
                                    case "slp": {
                                        String[] slpStr = splitLine[15].split(",");
                                        slpNot99999 = Integer.parseInt(slpStr[0]);//******
//                                        slpShouldBe1 = Integer.parseInt(slpStr[1]);//******
                                        slpShouldBe1 = Integer.parseInt(slpStr[1].replaceAll("\"", ""));//******
                                        if (slpShouldBe1==1 && slpNot99999 != 99999) {
                                            goodline=true;
                                        }
                                        break;
                                    }
                                }
//                                float lat = Float.parseFloat(splitLine[3]);//******
//                                float lon = Float.parseFloat(splitLine[4]);//******
//                                float elevation = Float.parseFloat(splitLine[5]);//******
//                                String[] windStr = splitLine[10].split(",");
//                                Integer windSpeedNot9999 = Integer.parseInt(windStr[3]); //******
//                                Integer windShouldBe1 = Integer.parseInt(windStr[4]); //******
//                                String[] visibilityStr = splitLine[12].split(",");
//                                Integer visibilityNot999999 = Integer.parseInt(visibilityStr[0]);//******
//                                Integer visibilityShouldBe1 = Integer.parseInt(visibilityStr[1]);//******
//                                String[] tempStr = splitLine[13].split(",");
//                                Integer tempNot9999 = Integer.parseInt(tempStr[0]);//******
//                                Integer tempShouldBe1 = Integer.parseInt(tempStr[1]);//******
//                                String[] dewStr = splitLine[14].split(",");
//                                Integer dewNot9999 = Integer.parseInt(dewStr[0]);//******
//                                Integer dewShouldBe1 = Integer.parseInt(dewStr[1]);//******
//                                String[] slpStr = splitLine[15].split(",");
//                                Integer slpNot99999 = Integer.parseInt(slpStr[0]);//******
//                                Integer slpShouldBe1 = Integer.parseInt(slpStr[1].replaceAll("\"", ""));//******

                                if (goodline){
                                    Measure m = new Measure(id, timestamp, lat, lon, elevation, windSpeedNot9999, visibilityNot999999, tempNot9999, dewNot9999, slpNot99999);
                                    thisFile.add(m);
                                    lastTime = timestamp;
                                    faultyLines=0;
                                } else {
                                    faultyLines++;
                                    if (faultyLines>20) {
                                        brokenId = true;
                                        goodfiles.remove(entryx.getName());
                                        break;
                                    }
                                }
                            }
                            if (!brokenId) {
                                ArrayList<Measure> measuresThisId = allMeasures.get(sensorId);
                                if (measuresThisId == null) allMeasures.put(sensorId, thisFile);
                                else measuresThisId.addAll(thisFile);
                            }
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                        goodfiles.remove(entryx.getName());
                        System.err.println("Will ignore file " + entryx.getName());
                    }
                }
            }
        }

        // i am now ready to print all files
        long minVal = Long.MAX_VALUE, maxVal = -Long.MAX_VALUE;
        for (Map.Entry<String, ArrayList<Measure>> e : allMeasures.entrySet()) {
            if (brokenIds.contains(e.getKey()) || e.getValue()==null || e.getValue().size()<10)  {
                brokenIds.add(e.getKey());
                e.setValue(null);
                continue;
            }
            ArrayList<Measure> list = e.getValue();
            Collections.sort(list);
            minVal = Math.min(minVal, list.get(0).timestamp);
            maxVal = Math.max(maxVal, list.get(list.size() - 1).timestamp);
        }
        for (String bid:brokenIds)
            allMeasures.remove(bid);
        brokenIds.clear();
        minVal = minVal + (maxVal - minVal) / 100;
        maxVal = maxVal - (maxVal - minVal) / 100;
        long discretizationStep = (maxVal - minVal) / discretization;
        // interpolate
        int nullItems=0;
        int notNullItems=0;
        for (Map.Entry<String, ArrayList<Measure>> e : allMeasures.entrySet()) {
            long pos = minVal;
            if (brokenIds.contains(e.getKey()) || e.getValue()==null || e.getValue().size()<10) continue;
            ArrayList<Measure> list = e.getValue();
            ArrayList<Measure> newList = new ArrayList<>(discretization);
            String id = e.getKey();
            Iterator<Measure> iter = list.iterator();
            int addedTheSame = 0;
            boolean broken = false;
            while (iter.hasNext() && pos<=maxVal) {
                Measure m = iter.next();
                addedTheSame = 0;
                while (m.timestamp >= pos) {
                    newList.add(new Measure(pos,m));
                    pos = pos + discretizationStep;
                    addedTheSame++;
                }
                if (addedTheSame > 2) {
                    broken = true;
                }
            }
            if (newList.size()<discretization)
                broken=true;
            if (broken) {
                e.setValue(null);
                brokenIds.add(e.getKey());
                nullItems++;
            } else {
                notNullItems++;
                e.setValue(newList);
            }
        }

        for (String bid:brokenIds)
            allMeasures.remove(bid);
        brokenIds.clear();

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("weather-processed." + discretization + "." + notNullItems+"."+attributeToExtract));

            bw.write("" + notNullItems + "\n");
            bw.write("" + 10 + "\n");
            bw.write("" + discretization + "\n");

            for (Map.Entry<String, ArrayList<Measure>> e : allMeasures.entrySet()) {
                bw.write("" + e.getKey() + "\n");
                ArrayList<Measure> m = e.getValue();
                int cnt=0;
                for (Measure m1:m) {
                    bw.write(m1.toString2() + "\n");
                    cnt++;
                    if (cnt==discretization) break;
                }
            }
            bw.flush(); bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        HashMap<String,ArrayList<Measure>> readback = new HashMap<>();
//        try {
//            BufferedReader br2 = new BufferedReader(new FileReader("weather-processed." + discretization + "." + notNullItems));
//            notNullItems = Integer.parseInt(br2.readLine());
//            int dimensions =Integer.parseInt(br2.readLine());
//            discretization = Integer.parseInt(br2.readLine());
//            String id = "";
//            while (br2.ready()) {
//                id = (br2.readLine());
//                ArrayList<Measure> measures = new ArrayList<>();
//                for (int cnt=0;cnt<discretization;cnt++) {
//                    measures.add(Measure.fromString2(id, br2.readLine()));
//                }
//                readback.put(id, measures);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static double[][] readWeather(String filename, int numItems, int numRounds, Attribute attr) {
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

    public static double[][] readWeather2(String filename, int numItems, int numRounds, Attribute attr, List<String> header) {
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
                header.add(e.getKey());
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

