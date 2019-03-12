package de.hhu.bsinfo.dxram.ms.tasks;

import de.hhu.bsinfo.dxram.boot.BootService;
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

    //private int NUM_THREADS;

    public UpdatePrTask(){}

    /*public UpdatePrTask(int num_threads){
        NUM_THREADS = num_threads;
    }*/

    @Override
    public int execute(TaskContext p_ctx) {
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        
        final AtomicInteger voteCnt = new AtomicInteger(0);
        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        localchunks.next();
        //Iterable<Long> iterable = () -> localchunks;
        /**Spliterator size known?**/
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> voteCnt.getAndAdd(updatePR(p_cid, p_ctx)));
        int ret = voteCnt.get();
        System.out.println("VOTES: " + voteCnt);
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

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
       // p_exporter.writeInt(NUM_THREADS);
    }

    @Override
    public void importObject(Importer p_importer) {
       // NUM_THREADS = p_importer.readInt(NUM_THREADS);
    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
