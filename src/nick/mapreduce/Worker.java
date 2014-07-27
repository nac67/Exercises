package nick.mapreduce;

import java.util.List;

public class Worker extends Thread {
    public enum Type {MAPPER, REDUCER};

    private Instructions instructions;
    private Type type;
    
    private Tuple mapInput;
    private List<Tuple> reduceInput;
    private List<List<Tuple>> mapResults;
    private List<Tuple> reduceResults;
    
    private Object listLock;
    private Object isTableComplete;
    

    public void setMapJob(Tuple input, 
            Instructions instructions,
            List<List<Tuple>> mapResults, 
            Object listLock,
            Object isTableComplete) {
        
        this.mapInput = input;
        this.instructions = instructions;
        this.mapResults = mapResults;
        this.listLock = listLock;
        this.isTableComplete = isTableComplete;
        this.type = Type.MAPPER;
    }
    
    public void setReduceJob(List<Tuple> input, 
            Instructions instructions,
            List<Tuple> reduceResults, 
            Object listLock,
            Object isTableComplete) {
        
        this.reduceInput = input;
        this.instructions = instructions;
        this.reduceResults = reduceResults;
        this.listLock = listLock;
        this.isTableComplete = isTableComplete;
        this.type = Type.REDUCER;
    }
    
    @Override
    public void run () {        
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
            isTableComplete.notifyAll();
        }
    }

}
