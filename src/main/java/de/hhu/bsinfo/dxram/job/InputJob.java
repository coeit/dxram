package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.data.IntegerChunk;
import de.hhu.bsinfo.dxram.chunk.data.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class InputJob extends AbstractJob {

    //public static final short MS_TYPE_ID = 3;

    private String m_filename;
    //private int m_vertexCount = 0;

    public InputJob() {}

    public InputJob(String p_filename){
        m_filename = p_filename;
    }

    /*public InputJob(String p_filename){
        m_filename = p_filename;
    }*/


    @Override
    public void execute() {
        ChunkService m_chunkService = getService(ChunkService.class);
        NameserviceService m_nameService = getService(NameserviceService.class);
        MasterSlaveComputeService m_msService = getService(MasterSlaveComputeService.class);
        BootService m_bootService = getService(BootService.class);

        int vertexCnt = 0;
        HashMap<Integer, PageRankInVertex> chunkMap = new HashMap<>();
        //String filename = "/home/cons/data_s.txt";
        //String filename = "/home/cons/Desktop/hollins.dat_";
        //m_vertexCount = 8;
        System.out.println(m_filename);
        //System.out.println(m_vertexCount);
        ArrayList<Short> slaveIDs = m_msService.getStatusMaster((short) 0).getConnectedSlaves();

        try(BufferedReader br = new BufferedReader(new FileReader(m_filename))){
            String line;
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");

                if(!chunkMap.containsKey(Integer.parseInt(split[0]))){
                    PageRankInVertex tmp = new PageRankInVertex(Integer.parseInt(split[0]));
                    chunkMap.put(Integer.parseInt(split[0]), tmp);
                    System.out.println(split[0] + " : " + ChunkID.toHexString(correspondingChunkID(Integer.parseInt(split[0]),slaveIDs)));
                    vertexCnt++;
                }
                if(!chunkMap.containsKey(Integer.parseInt(split[1]))){
                    PageRankInVertex tmp = new PageRankInVertex(Integer.parseInt(split[1]));
                    chunkMap.put(Integer.parseInt(split[1]), tmp);
                    System.out.println(split[1] + " : " + ChunkID.toHexString(correspondingChunkID(Integer.parseInt(split[1]),slaveIDs)));
                    vertexCnt++;
                }
                //increase outdeg with writelockpreop
                //Nodes without incoming ?
                 // ERRORs beim lesen
                chunkMap.get(Integer.parseInt(split[1])).addInEdge(correspondingChunkID(Integer.parseInt(split[0]),slaveIDs));
                //chunkMap.get(Integer.parseInt(split[1])).addInEdge(Integer.parseInt(split[0]));
                chunkMap.get(Integer.parseInt(split[0])).increment_outDeg();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        IntegerChunk vertexCount = new IntegerChunk(vertexCnt);
        m_chunkService.create().create(m_msService.getStatusMaster((short) 0 ).getMasterNodeId(),vertexCount);
        m_nameService.register(vertexCount,"vCnt");
        m_chunkService.put().put(vertexCount);

        //ArrayList<Short> connectedSlaves = m_msService.getStatusMaster((short) 0).getConnectedSlaves();
        int slaveIndex = 0;
        int chunkCnt = chunkMap.keySet().size();
        for (Integer vertexPR : chunkMap.keySet()) {
            //chunkMap.get(vertexPR).invokeVertexPR(chunkMap.keySet().size());
            chunkMap.get(vertexPR).invokeVertexPR(chunkCnt);
            m_chunkService.create().create(slaveIDs.get(slaveIndex % slaveIDs.size()),chunkMap.get(vertexPR));
            System.out.println(vertexPR + " :: " + ChunkID.toHexString(chunkMap.get(vertexPR).getID()));
            //m_nameService.register(chunkMap.get(vertexPR), vertexPR.toString());
            slaveIndex++;
            m_chunkService.put().put(chunkMap.get(vertexPR));

        }

    }

    public long correspondingChunkID(int p_vertex, ArrayList<Short> slaveIDs){
        int slaveCnt = slaveIDs.size();
        short nid = slaveIDs.get((short) ((p_vertex-1) % slaveCnt));
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);
    }

    /*public int get_vertexCount(){
        return m_vertexCount;
    }*/

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        m_filename = p_importer.readString(m_filename);
        //m_vertexCount = p_importer.readInt(m_vertexCount);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);

        p_exporter.writeString(m_filename);
        //p_exporter.writeInt(m_vertexCount);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_filename);
    }

}
