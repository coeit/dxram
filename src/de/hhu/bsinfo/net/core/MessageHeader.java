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

import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

class MessageHeader implements Importable {

    /* Header size:
     *  messageID + type + subtype + messageType and exclusivity + payloadSize
     *  3b        + 1b   + 1b      + 1b                          + 4b           = 10 bytes
     */
    private int m_messageID;
    private byte m_messageTypeExc;
    private byte m_type;
    private byte m_subtype;
    private int m_payloadSize;

    // Constructors

    /**
     * Creates an instance of MessageHeader
     */
    MessageHeader() {
    }

    int getMessageID() {
        return m_messageID;
    }

    byte getType() {
        return m_type;
    }

    byte getSubtype() {
        return m_subtype;
    }

    byte getMessageType() {
        return (byte) (m_messageTypeExc >> 4);
    }

    boolean isExclusive() {
        return (m_messageTypeExc & 0xF) == 1;
    }

    int getPayloadSize() {
        return m_payloadSize;
    }

    void clear() {
        m_messageID = 0;
        m_type = 0;
        m_subtype = 0;
        m_payloadSize = 0;
    }

    @Override
    public void importObject(Importer p_importer) {
        int tmp = p_importer.readByte((byte) 0);
        if (tmp != 0) {
            m_messageID |= (tmp & 0xFF) << 16;
        }
        tmp = p_importer.readByte((byte) 0);
        if (tmp != 0) {
            m_messageID |= (tmp & 0xFF) << 8;
        }
        tmp = p_importer.readByte((byte) 0);
        if (tmp != 0) {
            m_messageID |= tmp & 0xFF;
        }

        System.out.println("Importing " + m_messageID);

        m_type = p_importer.readByte(m_type);
        m_subtype = p_importer.readByte(m_subtype);
        m_messageTypeExc = p_importer.readByte(m_messageTypeExc);
        m_payloadSize = p_importer.readInt(m_payloadSize);
    }

    @Override
    public int sizeofObject() {
        return 10;
    }
}