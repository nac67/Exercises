package nick.mapreduce;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;

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
    private Semaphore queueLock;
    private Object needJobs;
    private Object isTableComplete;
    
    public boolean programRunning = true;
    
    
    public void configure(Instructions instructions,
            Queue<Tuple> mapInputQueue,
            Queue<List<Tuple>> reduceInputQueue,
            List<List<Tuple>> mapResults,
            List<Tuple> reduceResults,
            Object listLock,
            Semaphore queueLock,
            Object needJobs,
            Object isTableComplete) {
        
        this.instructions = instructions;
        this.mapInputQueue = mapInputQueue;
        this.reduceInputQueue = reduceInputQueue;
        this.mapResults = mapResults;
        this.reduceResults = reduceResults;
        this.listLock = listLock;
        this.queueLock = queueLock;
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
            
            /* By the time it reaches the poll, there definitely needs to be
             * Stuff inside the queue, without the queueLock it is possible
             * that it breaks out of the while loop and by the time it reaches
             * the poll, someone else has already polled. However, this thread
             * cannot hold onto the queueLock when it is waiting so thats why
             * it releases, waits, and then grabs the lock again.
             */
            queueLock.acquireUninterruptibly();
                while(listNotReady()){
                    synchronized(needJobs){
                        try {
                            queueLock.release();
                            needJobs.wait(); //wait until the queue has stuff
                            queueLock.acquireUninterruptibly();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                
                if(type == Type.MAPPER){
                    mapInput = mapInputQueue.poll();
                }else {
                    reduceInput = reduceInputQueue.poll();
                }
            queueLock.release();
            
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
