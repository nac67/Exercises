package nick.mapreduce;

import java.util.List;
import java.util.Queue;

/**
 * A worker thread that can perform multiple jobs of either
 * mapping or reducing.
 * 
 * @author Nick Cheng
 */
public class PoolWorker extends Thread {
    public enum Type {WAIT, MAPPER, REDUCER};

    private Instructions instructions;
    private Type type = Type.WAIT;
    
    private Queue<Tuple> mapInputQueue;
    private Queue<List<Tuple>> reduceInputQueue;
    private List<List<Tuple>> mapResults;
    private List<Tuple> reduceResults;
    
    private Object listLock;
    private Object needJobs;
    private Object isTableComplete;
    
    public boolean programRunning = true;
    
    
    public void configure(Instructions instructions,
            Queue<Tuple> mapInputQueue,
            Queue<List<Tuple>> reduceInputQueue,
            List<List<Tuple>> mapResults,
            List<Tuple> reduceResults,
            Object listLock,
            Object needJobs,
            Object isTableComplete) {
        
        this.instructions = instructions;
        this.mapInputQueue = mapInputQueue;
        this.reduceInputQueue = reduceInputQueue;
        this.mapResults = mapResults;
        this.reduceResults = reduceResults;
        this.listLock = listLock;
        this.needJobs = needJobs;
        this.isTableComplete = isTableComplete;
        
    }
    
    public void setMode(Type type){
        this.type = type;
    }
    
    @Override
    public void run () {
        while(programRunning){
            Tuple mapInput = null;
            List<Tuple> reduceInput = null;
            
            /*
             * needJobs serves both to protect against multiple access to
             * the queue, and also as the condition variable to communicate
             * that there are jobs in the queue.
             */
            synchronized(needJobs){
                while(listNotReady()){
                    try {
                        needJobs.wait(); //wait until the queue has stuff
                    } catch (InterruptedException e) {
                        return; //terminate thread
                    }
                }
                
                if(type == Type.MAPPER){
                    mapInput = mapInputQueue.poll();
                }else {
                    reduceInput = reduceInputQueue.poll();
                }
            }
            
            List<Tuple> resultM = null;
            Tuple resultR = null;
            
            if(type == Type.MAPPER){
                resultM = instructions.map(mapInput);
            } else {
                resultR = instructions.reduce(reduceInput);
            }
    
            synchronized (listLock) {
                if(type == Type.MAPPER){
                    mapResults.add(resultM);
                }else{
                    reduceResults.add(resultR);
                }
            }
            
            synchronized (isTableComplete){
                // let controller know that its table MIGHT be ready.
                isTableComplete.notifyAll();
            }
        }
    }

    private boolean listNotReady() {
        if(type == Type.WAIT) {
            return true;
        }else if(type == Type.MAPPER) {
            return mapInputQueue.size() == 0;
        }else if(type == Type.REDUCER) {
            return reduceInputQueue.size() == 0;
        }
        
        return true;
    }
}
