package nick.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;

import nick.mapreduce.PoolWorker.Type;

/**
 * Oversees the entire mapreduce job and controls all of the worker threads.
 * 
 * @author Nick Cheng
 */
public class MapReduceController {
    List<Tuple> inputLines;
    Instructions instructions;
    List<Tuple> output;
    
    /**
     * Add a new job to the controller.
     * 
     * @param inputLines
     *      A set of lines, each to be passed to a mapper to form K/Vs
     * @param map
     *      A function that takes an input line and forms K/Vs
     * @param reduce
     *      Each reducer takes all like-keys and processes their values
     * @return
     *      Itself, to chain function calls
     */
    public MapReduceController addJob(List<Tuple> inputLines, Instructions instructions) {
        this.inputLines = inputLines;
        this.instructions = instructions;
        return this;
    }
    
    /**
     * Execute the map reduce job. This version does not make use of concurrency.
     * It only uses one thread.
     * @return mapreduce output
     */
    public List<Tuple> executeSerially() {
        
        // Map stage
        List<Tuple> allPairs = new ArrayList<Tuple>();
        for(Tuple pair : inputLines) {
            allPairs.addAll(instructions.map(pair));
        }
        
        // Shuffle stage
        Map<String, List<Tuple>> data = shuffle(allPairs);
        
        // Reduce stage
        output = new ArrayList<Tuple>();
        
        for (Map.Entry<String, List<Tuple>> entry : data.entrySet()) {
            List<Tuple> pairs = entry.getValue();
            output.add(instructions.reduce(pairs));
        }
        
        //output will be returned, or can be retrieved from gatherResult
        return output;
    }
    
    /**
     * Creates one thread for each concurrent job, there is no upper limit
     * @return mapreduce output
     */
    public List<Tuple> executeConcurrently() {
        // Condition variable that communicates completion of workers jobs
        final Object isTableComplete = new Object();
        
        // Mutex lock that makes sure lists are consistent
        final Object listLock = new Object();
        
        //-------------------
        // MAP STAGE
        //-------------------

        // the reason for each worker adding one element to mapperResulsts
        // instead of addAll like in the serial version, is that if each
        // mapper adds one element, it is easy to tell when all mappers
        // are done by checking the length of the list.
        List<List<Tuple>> mapperResults = new ArrayList<List<Tuple>>();
        int neededJobs = inputLines.size();
        
        for (int i=0; i<neededJobs; i++) {
            Worker m = new Worker();
            m.setMapJob(inputLines.get(i), instructions, mapperResults, listLock, isTableComplete);
            m.start();
        }
        
        // wait until all workers are done mapping
        while (mapperResults.size() < neededJobs) {
            try {
                synchronized (isTableComplete){
                    isTableComplete.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
        
        // Gather all mappers work
        List<Tuple> allPairs = new ArrayList<Tuple>();
        for (List<Tuple> result : mapperResults) {
            allPairs.addAll(result);
        }
        
        
        //-------------------
        // SHUFFLE STAGE
        //-------------------
        Map<String, List<Tuple>> data = shuffle(allPairs);
        
        
        //-------------------
        // REDUCE STAGE
        //-------------------        
        List<Tuple> reducerResults = new ArrayList<Tuple>();
        neededJobs = data.size();
        
        for (Map.Entry<String, List<Tuple>> entry : data.entrySet()) {
            List<Tuple> pairs = entry.getValue();
            Worker r = new Worker();
            r.setReduceJob(pairs, instructions, reducerResults, listLock, isTableComplete);
            r.start();
        }
        
        
        // wait until all workers are done reducing
        while (reducerResults.size() < neededJobs) {
            try {
                synchronized (isTableComplete){
                    isTableComplete.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
        
        //output will be returned, or can be retrieved from gatherResult
        output = reducerResults;
        return output;
    }
    
    /**
     * Creates exactly "workers" number of threads
     * @param workers Number of threads to use
     * @return mapreduceoutput
     */
    public List<Tuple> executeThreadPool(int workers) {
        // Condition variable that communicates completion of all workers jobs
        final Object isTableComplete = new Object();
        
        // Condition variable that communicates if there are jobs ready in queue
        final Object needJobs = new Object();  
        
        // Mutex lock so that lists remain consistent
        final Object listLock = new Object();
        
        // Semaphore acting as a mutex lock to make sure queues stay consistent
        // see Worker.java for reasoning
        final Semaphore queueLock = new Semaphore(1);
        
        Queue<Tuple> mapInputQueue = new LinkedList<Tuple>();
        Queue<List<Tuple>> reduceInputQueue = new LinkedList<List<Tuple>>();
        
        List<List<Tuple>> mapperResults = new ArrayList<List<Tuple>>();
        List<Tuple> reducerResults = new ArrayList<Tuple>();
        
        // Set up the pool of workers
        PoolWorker[] pool = new PoolWorker[workers];
        for(int i=0;i<workers;i++){
            pool[i] = new PoolWorker();
            pool[i].configure(instructions, mapInputQueue, reduceInputQueue, mapperResults, reducerResults, listLock, queueLock, needJobs, isTableComplete);
            pool[i].start();
        }
        
        //-------------------
        // MAP STAGE
        //-------------------
        int neededJobs = inputLines.size();
        
        //set up mappers work queue
        //no need to lock since the workers are not checking for map jobs yet
        for(Tuple input : inputLines) {
            mapInputQueue.add(input);
        }
        
        // All workers will begin checking queue for map jobs once notified
        setAllModes(pool, PoolWorker.Type.MAPPER);
        
        // Notify workers
        synchronized (needJobs){
            needJobs.notifyAll();
        }
        
        // wait until all workers are done mapping
        while (mapperResults.size() < neededJobs) {
            try {
                synchronized (isTableComplete){
                    isTableComplete.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
        
        // Gather all mappers work
        List<Tuple> allPairs = new ArrayList<Tuple>();
        for (List<Tuple> result : mapperResults) {
            allPairs.addAll(result);
        }
        
        
        //-------------------
        // SHUFFLE STAGE
        //-------------------
        Map<String, List<Tuple>> data = shuffle(allPairs);
        
        
        //-------------------
        // REDUCE STAGE
        //-------------------
        
        neededJobs = data.size();
        
        // set up reducers work queue
        // no need to lock since the workers are not checking for reduce jobs yet
        for (Map.Entry<String, List<Tuple>> entry : data.entrySet()) {
            List<Tuple> pairs = entry.getValue();
            reduceInputQueue.add(pairs);
        }
        
        // all workers will begin checking for reduce jobs once notified
        setAllModes(pool, PoolWorker.Type.REDUCER);
        
        // notify all workers
        synchronized (needJobs){
            needJobs.notifyAll();
        }
        
        // wait until all workers are done reducing
        while (reducerResults.size() < neededJobs) {
            try {
                synchronized (isTableComplete){
                    isTableComplete.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
        
        // terminate all threads
        for(PoolWorker p : pool) {
            p.interrupt();
        }
        
        // output will be returned, or can be retrieved from gatherResult
        output = reducerResults;
        return output;
    }
    
    

    private void setAllModes(PoolWorker[] pool, Type type) {
        for(PoolWorker w : pool){
            w.setMode(type);
        }
    }

    /**
     * Performs the shuffle stage between the map and reduce stages
     * @param allPairs
     *      all results from the map stage
     * @return
     *      Tuples grouped by key
     */
    private Map<String, List<Tuple>> shuffle(List<Tuple> allPairs) {
        Map<String, List<Tuple>> data = new HashMap<String, List<Tuple>>();
        
        for(Tuple pair : allPairs) {
            if(data.containsKey(pair.fst())){
                List<Tuple> entries = data.get(pair.fst());
                entries.add(pair);
            }else{
                List<Tuple> entries = new ArrayList<Tuple>();
                entries.add(pair);
                data.put(pair.fst(), entries);
            }
        }
        
        return data;
    }
    
    /**
     * Get the result to the last executed mapReduce job
     * @return result to last executed mapReduce job
     */
    public List<Tuple> gatherResult () {
        return output;
    }
}
