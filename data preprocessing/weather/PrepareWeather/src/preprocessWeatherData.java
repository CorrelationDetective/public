import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class preprocessWeatherData {
    public static boolean excludeFileName(String filename) {
        return filename.hashCode()%fileSelectionProbability!=0;
    }
    static int fileSelectionProbability = 4000;
    static int readingsPerDay=4;


    // parameters are
    // 1. input folder where the tar.gz files are located (their file name is year.tar.gz). You do not need to extract the tar files.
    // 2. second parameter is the attribute that needs to be extracted. a) temp for temperature, b) slp for sea-level pressure
    // 3. start year -- inclusive
    // 4. end year -- inclusive
    // 5. testing parameter -- set always to 1000
    // 6. readings per day -- for our experiments it was set to 4
    // notice that the reader does not work with recent
    public static void main(String[]args) throws IOException {
        if (args == null || args.length == 0) {

            args = new String[]{
                    "/Users/papapetrou/iCloud/datasets/noaa/", "temp",
                    "2000", "2005", "1000", "4" //317565144720
            };
        }
        String filesPath = args[0];
        String attributeToExtract = args[1];
        int yearIntStart = Integer.parseInt(args[2]);
        int yearIntStop = Integer.parseInt(args[3]);
        fileSelectionProbability = Integer.parseInt(args[4]);
        readingsPerDay = Integer.parseInt(args[5]);
        long dayDuration = 24 * 60 * 60 * 1000; // one day
        long periodDurationInMillis = dayDuration/readingsPerDay;
        BufferedReader br=null;
        // now load files
        HashMap<String, ArrayList<Measure>> allMeasures = new HashMap<>();

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
                    if (fileread++%1000==999)
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
                                line = line.replaceAll(",,", ",\"\",");

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
                                } else if (timestamp - 10*dayDuration > lastTime && lastTime != 0) { // more than 10 days
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
                        //   e.printStackTrace();
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
//        minVal = minVal + (maxVal - minVal) / 50; // remove 2% from the beginning and end
//        maxVal = maxVal - (maxVal - minVal) / 50; // remove 2% from the beginning and end
        long discretizationStep = periodDurationInMillis;
        int vectorSize = (yearIntStop-yearIntStart+1)*366*readingsPerDay;
        // interpolate
        int nullItems=0;
        int notNullItems=0;
        final int constantSize = (int)Math.floor((maxVal-minVal)/periodDurationInMillis);
        for (Map.Entry<String, ArrayList<Measure>> e : allMeasures.entrySet()) {
            long pos = minVal;
            if (brokenIds.contains(e.getKey()) || e.getValue()==null || e.getValue().size()<10) continue;
            ArrayList<Measure> list = e.getValue();
            ArrayList<Measure> newList = new ArrayList<>(vectorSize);
            String id = e.getKey();
            Iterator<Measure> iter = list.iterator();
            int addedTheSame = 0;
            boolean broken = false;
            if (!iter.hasNext()) continue;
            Measure m = iter.next();
            while (iter.hasNext() && m.timestamp<pos) m = iter.next();
            if (!iter.hasNext()) continue; // else i am at the correct position to start adding
            newList.add(new Measure(pos,m));
            pos = pos + discretizationStep;

            while (iter.hasNext() && pos<=maxVal && newList.size()<constantSize) {
                m = iter.next();
                addedTheSame = 0;
                while ((m.timestamp >= pos && newList.size()<constantSize) || (!iter.hasNext() && newList.size()<constantSize)) {
                    newList.add(new Measure(pos,m));
                    pos = pos + discretizationStep;
                    addedTheSame++;
                }
                if (addedTheSame > 5*readingsPerDay) {
                    broken = true;
                }
            }
            int totalRecords = newList.size();
            if (totalRecords!=constantSize) broken=true; // stream not complete
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
        System.err.println("Total results " + notNullItems + " of size " + constantSize);
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("weather-processed." + readingsPerDay + "." + notNullItems+"."+attributeToExtract));

            bw.write("" + notNullItems + "\n");
            bw.write("" + 10 + "\n");
            bw.write("" + constantSize + "\n");

            for (Map.Entry<String, ArrayList<Measure>> e : allMeasures.entrySet()) {
                bw.write("" + e.getKey() + "\n");
                ArrayList<Measure> m = e.getValue();
                int cnt=0;
                for (Measure m1:m) {
                    bw.write(m1.toString2() + "\n");
                    cnt++;
                    if (cnt==constantSize) break;
                }
            }
            bw.flush(); bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

