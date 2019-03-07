package de.hhu.bsinfo.dxram.chunk.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class PageRankInVertex extends AbstractChunk {
    //private int[] m_inEdges = new int[0];
    private long[] m_inEdges = new long[0];
    private double m_currPR = 0.0;
    private double m_tmpPR = 0.0;
    private int m_outDeg = 0;
    private int m_name;


    public PageRankInVertex(int p_name) {
        super();
        m_name = p_name;
    }

    public PageRankInVertex(final long p_id){
        super(p_id);
    }

    public void invokeVertexPR(int N) {
        m_currPR = 1/(double) N;
        if(m_outDeg == 0){
            m_outDeg = N;
        }
    }

    public void increment_outDeg(){
        m_outDeg++;
    }

    public void increment_outDeg(int num){
        m_outDeg += num;
    }

    public void addInEdge(final long p_neighbour) {
        setInCnt(m_inEdges.length + 1);
        m_inEdges[m_inEdges.length - 1] = p_neighbour;
    }

    public void setInCnt(final int p_cnt) {
        if (p_cnt != m_inEdges.length) {
            m_inEdges = Arrays.copyOf(m_inEdges, p_cnt);
        }

    }

    public void addIncPR(double p_PR){
        m_tmpPR += p_PR;
    }

    public void calcPR(int N, double D){
        m_tmpPR = (1 - D)/N + D * m_tmpPR;
    }

    public void updatePR(){
        m_currPR = m_tmpPR;
        m_tmpPR = 0.0;
    }

    public int getM_outDeg(){return m_outDeg;}

    public double getM_currPR(){
        return m_currPR;
    }

    public double getM_tmpPR(){
        return m_tmpPR;
    }
    /*public double getM_prevPR(){
        return m_prevPR;
    }*/

    public void setM_currPR(double p_newPR){
        m_currPR = p_newPR;
    }

    /*public void setM_prevPR(){
        m_prevPR = m_currPR;
    }*/

    public long[] getM_inEdges(){
        return m_inEdges;
    }


    public int get_name(){
        return m_name;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeDouble(m_currPR);
        p_exporter.writeDouble(m_tmpPR);
        p_exporter.writeInt(m_outDeg);
        p_exporter.writeInt(m_name);
        /*for(String s : m_inEdges){
            p_exporter.writeString(s);
        }*/
        //p_exporter.writeIntArray(m_inEdges);
        p_exporter.writeLongArray(m_inEdges);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_currPR = p_importer.readDouble(m_currPR);
        m_tmpPR = p_importer.readDouble(m_tmpPR);
        m_outDeg = p_importer.readInt(m_outDeg);
        m_name = p_importer.readInt(m_name);
        /*for (int i = 0; i < m_inEdges.length; i++) {
            m_inEdges[i] = p_importer.readString(m_inEdges[i]);
        }*/
        //m_inEdges = p_importer.readIntArray(m_inEdges);
        m_inEdges = p_importer.readLongArray(m_inEdges);
    }

    @Override
    public int sizeofObject() {
        //int sum = 0;
        /*for (String s : m_inEdges){
            sum += ObjectSizeUtil.sizeofString(s);
        }*/
        return Double.BYTES * 2 + Integer.BYTES * 2 + ObjectSizeUtil.sizeofLongArray(m_inEdges);
    }
}
