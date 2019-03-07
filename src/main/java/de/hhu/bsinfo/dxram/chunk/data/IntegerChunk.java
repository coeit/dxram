package de.hhu.bsinfo.dxram.chunk.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import jdk.nashorn.internal.objects.annotations.Getter;

public class IntegerChunk extends AbstractChunk {

    private int m_value;

    public IntegerChunk(int m_value) {
        super();
        this.m_value = m_value;
    }

    public IntegerChunk(long p_chunkID) {
        super(p_chunkID);
    }

    public int getM_value() {
        return m_value;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_value);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_value = p_importer.readInt(m_value);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}
