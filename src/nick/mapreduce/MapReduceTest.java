package nick.mapreduce;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class MapReduceTest {

    /**
     * Input file contains multiple lines of the format
     * kid1 class1  grade1  class2  grade2...
     * kid2 class1' grade1' class2' grade2'...
     * 
     * Each mapper handles one kid, and outputs a list of <class, grade> 
     * for each class that kid takes
     * 
     * Each reducer will then handle one class (the shuffle stage gathers 
     * all the kids who've taken that class) and calculates the average
     * grade for the class.
     * 
     * The final output is a list of classes with their average scores.
     * 
     * This version is serial, meaning there are no concurrent threads.
     */    
    @Test
    public void testAverageGradeSerial() throws FileNotFoundException, IOException {
        List<Tuple> input = getTuplesFromFile("mapreduceExamples/courses.txt");
        
        MapReduceController mr = new MapReduceController();
        
        mr.addJob(input, new Instructions() {
            
            @Override
            public List<Tuple> map(Tuple input) {
                return avgGradeMap(input);
            }
            
            @Override
            public Tuple reduce(List<Tuple> input) {
                return avgGradeReduce(input);
            }
            
        }).executeSerially();
        
        
        List<Tuple> output = mr.gatherResult();
        
        assertEquals(5, output.size());
        for(Tuple tup : output) {
            //System.out.println(tup);
            assertTuple(tup, "Juggling", "90");
            assertTuple(tup, "History", "86");
            assertTuple(tup, "Math", "92");
            assertTuple(tup, "Science", "92");
            assertTuple(tup, "English", "85");
        }
    }

    
    
    @Test
    public void testAverageGradeConcurrent() throws FileNotFoundException, IOException {
        List<Tuple> input = getTuplesFromFile("mapreduceExamples/courses.txt");
        
        MapReduceController mr = new MapReduceController();
        
        mr.addJob(input, new Instructions() {
            
            @Override
            public List<Tuple> map(Tuple input) {
                return avgGradeMap(input);
            }
            
            @Override
            public Tuple reduce(List<Tuple> input) {
                return avgGradeReduce(input);
            }
            
        }).executeConcurrently();
        
        
        List<Tuple> output = mr.gatherResult();
        
        assertEquals(5, output.size());
        for(Tuple tup : output) {
            //System.out.println(tup);
            assertTuple(tup, "Juggling", "90");
            assertTuple(tup, "History", "86");
            assertTuple(tup, "Math", "92");
            assertTuple(tup, "Science", "92");
            assertTuple(tup, "English", "85");
        }
    }
    
    @Test
    public void testAverageGradePool() throws FileNotFoundException, IOException {
        List<Tuple> input = getTuplesFromFile("mapreduceExamples/courses.txt");
        
        MapReduceController mr = new MapReduceController();
        
        mr.addJob(input, new Instructions() {
            
            @Override
            public List<Tuple> map(Tuple input) {
                return avgGradeMap(input);
            }
            
            @Override
            public Tuple reduce(List<Tuple> input) {
                return avgGradeReduce(input);
            }
            
        }).executeThreadPool(2);
        
        
        List<Tuple> output = mr.gatherResult();
        
        assertEquals(5, output.size());
        for(Tuple tup : output) {
            //System.out.println(tup);
            assertTuple(tup, "Juggling", "90");
            assertTuple(tup, "History", "86");
            assertTuple(tup, "Math", "92");
            assertTuple(tup, "Science", "92");
            assertTuple(tup, "English", "85");
        }
    }
    
    private List<Tuple> avgGradeMap(Tuple input) {
        // input is coming in as:
        // kidName class1 grade1 class2 grade2...
          
        List<Tuple> output = new ArrayList<Tuple>();
        
        String pairs = input.snd(); //class1 grade1 class2 grade2...
        
        String[] classAndGrade = pairs.split("\\s+");
        for(int i=0;i<classAndGrade.length;i+=2){
            output.add(new Tuple(classAndGrade[i],classAndGrade[i+1]));
        }
        
        lolligag();
        
        //<class1, grade1>, <class2, grade2> ...
        return output;
    }
    
    private Tuple avgGradeReduce(List<Tuple> input) {
        // input is coming in as 
        // <same class, grade from one kid>, <same class, grade from another class>
        int sum = 0;
        String className = input.get(0).fst();
        
        for(Tuple classAndGrade : input) {
            sum += Integer.parseInt(classAndGrade.snd());
        }
        
        Integer average = Math.round((float) sum / input.size());
        
        lolligag();
        
        // output <same class, average grade from all kids>
        return new Tuple(className, average.toString());
    }
    
    
    /**
     * Input file contains multiple lines of the format
     * city highTemp lowTemp
     * city highTemp lowTemp
     * 
     * Each mapper handles one city, and outputs a tuple of <state, hi low> 
     * 
     * Each reducer will then handle one state (the shuffle stage gathers 
     * all the states into groups) and find overall high and low
     * 
     * The final output is a list of states with their highs and lows.
     */
    @Test
    public void testWeatherPool() throws FileNotFoundException, IOException {
        List<Tuple> input = getTuplesFromFile("mapreduceExamples/weather.txt");
        
        MapReduceController mr = new MapReduceController();
        
        mr.addJob(input, new Instructions() {
            
            @Override
            public List<Tuple> map(Tuple input) {
                return weatherMap(input);
            }
            
            @Override
            public Tuple reduce(List<Tuple> input) {
                return weatherReduce(input);
            }
            
        }).executeThreadPool(2);
        
        
        List<Tuple> output = mr.gatherResult();
        
        assertEquals(3, output.size());
        for(Tuple tup : output) {
            assertTuple(tup, "PA", "86 54");
            assertTuple(tup, "TX", "97 75");
            assertTuple(tup, "CA", "106 61");
        }
    }
    
    
    private List<Tuple> weatherMap(Tuple input) {
        // input is coming in as:
        // city high low
        
        Map<String, String> mockLocationService = new HashMap<String, String>();
        mockLocationService.put("Harrisburg", "PA");
        mockLocationService.put("Pittsburgh", "PA");
        mockLocationService.put("Phildelphia", "PA");
        mockLocationService.put("Houston", "TX");
        mockLocationService.put("SanAntonio", "TX");
        mockLocationService.put("Austin", "TX");
        mockLocationService.put("Sacramento", "CA");
        mockLocationService.put("LosAngeles", "CA");
        mockLocationService.put("SanFransico", "CA");
          
        List<Tuple> output = new ArrayList<Tuple>();
        
        String city = input.fst();
        String hiLow = input.snd();
        
        output.add(new Tuple(mockLocationService.get(city), hiLow));
        
        lolligag();
        
        //<state, hi low>
        return output;
    }
    
    private Tuple weatherReduce(List<Tuple> input) {
        // input is coming in as 
        // <same state, hilow from one city>, <same state, hilow from another city>...
        String state = input.get(0).fst();
        
        Integer highest = Integer.MIN_VALUE;
        Integer lowest = Integer.MAX_VALUE;
        
        for(Tuple pair : input) {
            String[] hiAndLow = pair.snd().split("\\s+");
            int hi = Integer.parseInt(hiAndLow[0]);
            int lo = Integer.parseInt(hiAndLow[1]);
            
            if(hi>highest) {
                highest = hi;
            }
            
            if(lo<lowest) {
                lowest = lo;
            }
        }
        
        
        lolligag();
        
        // output <same state, state's hi low>
        return new Tuple(state, highest.toString()+" "+lowest.toString());
    }
    
    
    
    //-------------------------HELPER-METHODS------------------------------------
    
    private List<Tuple> getTuplesFromFile(String file) throws IOException, FileNotFoundException {
        List<Tuple> input = new ArrayList<Tuple>();
        
        try(BufferedReader reader = new BufferedReader(new FileReader(new File(file)))){
            for(String line = reader.readLine(); line != null; line = reader.readLine()){
                int firstSpace = line.indexOf(' ');
                input.add( new Tuple(line.substring(0, firstSpace), line.substring(firstSpace+1)) );
            }
        }
        return input;
    }

    private void assertTuple(Tuple tup, String key, String value) {
        if(tup.fst().equals(key)){
            assertEquals(tup.snd(), value);
        }
    }
    
    private void lolligag () {
        try {
            Thread.sleep((long) (Math.random()*30+30));
        } catch (InterruptedException e) {}
    }

}
