package de.hhu.bsinfo.dxram.ms.tasks;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.data.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceEntryStr;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SendPrTask implements Task {

    private int NUM_THREADS;
    private int N;
    private double DAMP;

    public SendPrTask(){}

    public SendPrTask(int num_threads, int vertexCount, double damping_factor){
        NUM_THREADS = num_threads;
        DAMP = damping_factor;
        N = vertexCount;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        BootService m_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService m_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService m_nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        /*String nodeID = NodeID.toHexString(m_bootService.getNodeID());
        String nodeIDs = NodeID.toHexString(m_bootService.getNodeID()).substring(2,6);
        LocalPageRankVertices localPageRankVertices = new LocalPageRankVertices(m_nameService.getChunkID(nodeIDs, 333));
        m_chunkService.get().get(localPageRankVertices);*/



        /*ArrayList<Long> localchunks = new ArrayList<>();
        for (Integer vertex : localPageRankVertices.getM_localVertices()){
            localchunks.add(m_nameService.getChunkID(Integer.toString(vertex),333));
            //System.out.print(vertex + " ");
        }
        System.out.println();*/

        /*ArrayList<Long> localchunks = new ArrayList<>();
        for (NameserviceEntryStr name : m_nameService.getAllEntries()){
            //System.out.println(ChunkID.toHexString(name.getValue()));
            if(ChunkID.toHexString(name.getValue()).startsWith(nodeID)) {//&& isInt(ChunkID.toHexString(name.getValue()))){
                localchunks.add(name.getValue());
            }
        }*/

        /*Thread t1 = new Thread(new SendPrRunnable(localchunks.get(0), p_ctx));
        Thread t2 = new Thread(new SendPrRunnable(localchunks.get(1), p_ctx));
        t1.start();
        try {
            t1.join();
            System.out.println("t1 f");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        t2.start();
        try {
            t2.join();
            System.out.println("t2 f");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        //ChunkIDRanges localChunkIDRangesIt = m_chunkService.cidStatus().getAllLocalChunkIDRanges(m_bootService.getNodeID());

        Iterator<Long> localchunks = m_chunkService.cidStatus().getAllLocalChunkIDRanges(m_bootService.getNodeID()).iterator();
        localchunks.next();
        Iterable<Long> iterable = () -> localchunks;
        //Stream.of(iterable.spliterator()).parallel().forEach(p_cid -> sendPr(p_cid,p_ctx,N,DAMP));
        //StreamSupport.stream(iterable.spliterator(),false).parallel().forEach(p_cid -> sendPr(p_cid,p_ctx,N,DAMP));
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> sendPr(p_cid,p_ctx,N,DAMP));
        /*System.out.println("NUM THREADS:" + NUM_THREADS);
        ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
        int active;
        long vertex = localchunks.next();
        while (localchunks.hasNext()){
            vertex = localchunks.next();
            active = activeThreads(threads);
            if (active < NUM_THREADS){
                Thread thread = new Thread(new SendPrRunnable(vertex,p_ctx,N,DAMP));
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
        return 0;
    }

    public void sendPr(Long p_cid, TaskContext p_ctx, int vertexCount, double damping){

        ChunkService m_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        PageRankInVertex m_vertex = new PageRankInVertex(p_cid);


        m_chunkService.get().get(m_vertex);
        //System.out.println(ChunkID.toHexString(m_vertex.getID()));
        long incidenceList[] = m_vertex.getM_inEdges();
        for (int i = 0; i < incidenceList.length; i++) {
            //System.out.print("---" + ChunkID.toHexString(incidenceList[i]) + "---");
            PageRankInVertex tmpChunk = new PageRankInVertex(incidenceList[i]);
            m_chunkService.get().get(tmpChunk);
            m_vertex.addIncPR(tmpChunk.getM_currPR()/(double)tmpChunk.getM_outDeg());
            //System.out.println(" :: " + tmpChunk.getM_currPR() + " .. " + tmpChunk.getM_outDeg());
        }
        m_vertex.calcPR(N,DAMP);
        //System.out.println("# " + m_vertex.getM_tmpPR());
        m_chunkService.put().put(m_vertex);
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
        p_exporter.writeInt(N);
        p_exporter.writeDouble(DAMP);
    }

    @Override
    public void importObject(Importer p_importer) {
        NUM_THREADS = p_importer.readInt(NUM_THREADS);
        N = p_importer.readInt(N);
        DAMP = p_importer.readDouble(DAMP);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES*2 + Double.BYTES;
    }
}
