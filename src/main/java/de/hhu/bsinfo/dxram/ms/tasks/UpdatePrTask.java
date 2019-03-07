package de.hhu.bsinfo.dxram.ms.tasks;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.data.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class UpdatePrTask implements Task {

    private int NUM_THREADS;

    public UpdatePrTask(){}

    public UpdatePrTask(int num_threads){
        NUM_THREADS = num_threads;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        BootService m_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService m_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService m_nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);


        /*String nodeID = NodeID.toHexString(m_bootService.getNodeID());
        ArrayList<Long> localchunks = new ArrayList<>();
        for (NameserviceEntryStr name : m_nameService.getAllEntries()){
            //System.out.println(ChunkID.toHexString(name.getValue()));
            if(ChunkID.toHexString(name.getValue()).startsWith(nodeID)) {//&& isInt(ChunkID.toHexString(name.getValue()))){
                localchunks.add(name.getValue());
            }
        }*/
        final AtomicInteger voteCnt = new AtomicInteger(0);
        Iterator<Long> localchunks = m_chunkService.cidStatus().getAllLocalChunkIDRanges(m_bootService.getNodeID()).iterator();
        localchunks.next();
        //Iterable<Long> iterable = () -> localchunks;
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> voteCnt.getAndAdd(updatePR(p_cid, p_ctx)));
        /*StreamSupport.stream(iterable.spliterator(),false).parallel().forEach(p_cid -> {
            //AtomicInteger vote = updatePR(p_cid, p_ctx);
            voteCnt.getAndAdd(updatePR(p_cid, p_ctx));});
          */
        int ret = voteCnt.get();
        System.out.println("VOTES: " + voteCnt);
        /*ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
        int active;
        long vertex = localchunks.next();
        System.out.println("NUM THREADS: " + NUM_THREADS);
        while (localchunks.hasNext()){
            vertex = localchunks.next();
            active = activeThreads(threads);
            if (active < NUM_THREADS){
                Thread thread = new Thread(new UpdatePrRunnable(vertex,p_ctx));
                threads.add(thread);
                thread.start();
                System.out.println("Thread started " + active);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        for (Thread thread : threads){
            try {
                thread.join();
                System.out.println("Thread finished");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
        return ret;
    }

    public int updatePR(Long p_cid, TaskContext p_ctx){
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        //ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        PageRankInVertex vertex = new PageRankInVertex(p_cid);
        chunkService.get().get(vertex);
        //chunkLocalService.getLocal().get(vertex);
        double err = vertex.getM_currPR() - vertex.getM_tmpPR();
        int ret = 0;
        if(Math.abs(err) < 0.02){ ret = 1;}
        vertex.updatePR();
        chunkService.put().put(vertex);
        System.out.println(vertex.get_name() + " VOTE: " + ret);
        return ret;
    }

    public int activeThreads(ArrayList<Thread> threads){
        int ret = 0;
        for(Thread thread: threads){
            if(thread.isAlive()){
                ret++;
            }
        }
        return ret;
    }

    public boolean isInt(String str){
        try{
            int i = Integer.parseInt(str);
        } catch (NumberFormatException | NullPointerException nf){
            return false;
        }
        return true;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(NUM_THREADS);
    }

    @Override
    public void importObject(Importer p_importer) {
        NUM_THREADS = p_importer.readInt(NUM_THREADS);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}
