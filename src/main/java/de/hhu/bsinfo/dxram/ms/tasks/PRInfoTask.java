package de.hhu.bsinfo.dxram.ms.tasks;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.data.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
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

public class PRInfoTask implements Task {

    public PRInfoTask(){
    }

    @Override
    public int execute(TaskContext p_ctx) {
        ChunkService m_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService m_nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        BootService m_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        MasterSlaveComputeService m_computeService = p_ctx.getDXRAMServiceAccessor().getService(MasterSlaveComputeService.class);

        ArrayList<String> nids = new ArrayList<>();

        for (short nid : m_bootService.getOnlinePeerNodeIDs()){
            String n_id = NodeID.toHexString(nid).substring(2,6);
            nids.add(n_id);
        }

        ArrayList<Short> slaveIDs = m_computeService.getStatusMaster((short) 0).getConnectedSlaves();


        for (short slaveID : slaveIDs){
            Iterator<Long> localChunks = m_chunkService.cidStatus().getAllLocalChunkIDRanges(slaveID).iterator();
            //if (!vertexPR.getName().equals(NodeID.toHexString(m_bootService.getNodeID()).substring(2,6))) {
            //if(!nids.contains(vertexPR.getName())){
            localChunks.next();
            while(localChunks.hasNext()){
                //PageRankInVertex vert = new PageRankInVertex(m_nameService.getChunkID(vertexPR.getName(),100));
                PageRankInVertex vert = new PageRankInVertex(localChunks.next());
                m_chunkService.get().get(vert);
		        System.out.print(vert.get_name() + ": ");
                //System.out.print(vertexPR.getName() + ": " + vert.getM_currPR());
                //System.out.print(vertexPR.getName() + ": " + ChunkID.toHexString(m_nameService.getChunkID(vertexPR.getName(),100)) + ": ");
                for (Long i : vert.getM_inEdges()){
                    System.out.print(ChunkID.toHexString(i) + " ");
                }
                System.out.println("\noutdeg: " + vert.getM_outDeg());// + " " + vert.get_indegAlt());
                System.out.println("PR: " + vert.getM_currPR());
                System.out.println("=====");
            }

        }



        /*for (NameserviceEntryStr vertexPR : m_nameService.getAllEntries()){

            //if (!vertexPR.getName().equals(NodeID.toHexString(m_bootService.getNodeID()).substring(2,6))) {
            if(!nids.contains(vertexPR.getName())){
                PageRankInVertex vert = new PageRankInVertex(m_nameService.getChunkID(vertexPR.getName(),100));
                m_chunkService.get().get(vert);
                //System.out.print(vertexPR.getName() + ": " + vert.getM_currPR());
                //System.out.print(vertexPR.getName() + ": " + ChunkID.toHexString(m_nameService.getChunkID(vertexPR.getName(),100)) + ": ");
                for (Long i : vert.getM_inEdges()){
                    System.out.print(i + " ");
                }
                System.out.println("\noutdeg: " + vert.getM_outDeg());// + " " + vert.get_indegAlt());
                System.out.println("PR: " + vert.getM_currPR());
                System.out.println("size: " + vert.sizeofObject());
                System.out.println("=====");
            }

        }*/

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public void importObject(Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
