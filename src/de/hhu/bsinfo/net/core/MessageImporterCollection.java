/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.net.core;

/**
 * Importer collection.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class MessageImporterCollection {

    private MessageImporterDefault m_importer;
    private MessageImporterOverflow m_importerOverflow;
    private MessageImporterUnderflow m_importerUnderflow;
    private MessageImporterUnderOverflow m_importerUnderOverflow;

    private UnfinishedImporterOperation m_unfinishedOperation;

    private int m_bytesCopied;

    /**
     * Constructor
     */
    MessageImporterCollection() {
        super();

        m_unfinishedOperation = new UnfinishedImporterOperation();

        m_importer = new MessageImporterDefault();
        m_importerOverflow = new MessageImporterOverflow(m_unfinishedOperation);
        m_importerUnderflow = new MessageImporterUnderflow(m_unfinishedOperation);
        m_importerUnderOverflow = new MessageImporterUnderOverflow(m_unfinishedOperation);

        m_bytesCopied = 0;
    }

    AbstractMessageImporter getImporter(final long p_addr, final int p_size, final int p_position, final boolean p_hasOverflow) {
        AbstractMessageImporter ret;

        if (m_bytesCopied != 0) {
            // TODO: under-overflow
            System.out.println("underflow");
            ret = m_importerUnderflow;
        } else if (p_hasOverflow) {
            ret = m_importerOverflow;
            System.out.println("overflow");
        } else {
            ret = m_importer;
        }
        // mirror ByteBuffer position and limit (range) to importer
        ret.setBuffer(p_addr, p_size, p_position);
        ret.setNumberOfReadBytes(m_bytesCopied);

        return ret;
    }

    int returnImporter(final AbstractMessageImporter p_importer, final boolean p_finished) {
        if (p_finished) {
            m_bytesCopied = 0;
            m_unfinishedOperation.reset();
        } else {
            m_bytesCopied = p_importer.getNumberOfReadBytes();
        }

        return p_importer.getNumberOfReadBytes();
    }

}