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

package de.hhu.bsinfo.dxram.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationRequest;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.messages.LogAnonMessage;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.log.tcmd.TcmdLoginfo;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class LogService extends AbstractDXRAMService implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogService.class.getSimpleName());

    // dependent components
    private NetworkComponent m_network;
    private LogComponent m_log;
    private TerminalComponent m_terminal;

    /**
     * Constructor
     */
    public LogService() {
        super("log");
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization() {
        return m_log.getCurrentUtilization();
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization(final short p_nid) {
        GetUtilizationRequest request = new GetUtilizationRequest(p_nid);

        try {
            m_network.sendSync(request);
        } catch (NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending GetUtilizationRequest failed", e);
            // #endif /* LOGGER >= ERROR */
        }

        GetUtilizationResponse response = (GetUtilizationResponse) request.getResponse();
        return response.getUtilization();
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOG_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LogMessages.SUBTYPE_LOG_MESSAGE:
                        incomingLogMessage((LogMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_LOG_BUFFER_MESSAGE:
                        incomingLogBufferMessage((LogAnonMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_REMOVE_MESSAGE:
                        incomingRemoveMessage((RemoveMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_INIT_REQUEST:
                        incomingInitRequest((InitRequest) p_message);
                        break;
                    case LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST:
                        incomingGetUtilizationRequest((GetUtilizationRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Handles an incoming GetUtilizationRequest
     * Method is used in js script.
     *
     * @param p_request
     *     the GetUtilizationRequest
     */
    @SuppressWarnings("WeakerAccess")
    public void incomingGetUtilizationRequest(final GetUtilizationRequest p_request) {

        try {
            m_network.sendMessage(new GetUtilizationResponse(p_request, getCurrentUtilization()));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not answer GetUtilizationRequest", e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
        m_terminal = p_componentAccessor.getComponent(TerminalComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        registerNetworkMessages();
        registerNetworkMessageListener();
        registerTerminalCommands();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Handles an incoming LogMessage
     *
     * @param p_message
     *     the LogMessage
     */
    private void incomingLogMessage(final LogMessage p_message) {
        m_log.incomingLogChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming LogAnonMessage
     *
     * @param p_message
     *     the LogAnonMessage
     */
    private void incomingLogBufferMessage(final LogAnonMessage p_message) {
        m_log.incomingLogChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming RemoveMessage
     *
     * @param p_message
     *     the RemoveMessage
     */
    private void incomingRemoveMessage(final RemoveMessage p_message) {
        m_log.incomingRemoveChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming InitRequest
     *
     * @param p_request
     *     the InitRequest
     */
    private void incomingInitRequest(final InitRequest p_request) {
        boolean res;

        res = m_log.incomingInitBackupRange(p_request.getRangeID(), p_request.getSource());

        try {
            m_network.sendMessage(new InitResponse(p_request, res));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not acknowledge initilization of backup range", e);
            // #endif /* LOGGER >= ERROR */

        }
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_BUFFER_MESSAGE, LogMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, RemoveMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST, GetUtilizationRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_RESPONSE, GetUtilizationResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(LogMessage.class, this);
        m_network.register(LogAnonMessage.class, this);
        m_network.register(RemoveMessage.class, this);
        m_network.register(InitRequest.class, this);
        m_network.register(GetUtilizationRequest.class, this);
    }

    /**
     * Register terminal commands
     */
    private void registerTerminalCommands() {
        m_terminal.registerTerminalCommand(new TcmdLoginfo());
    }
}
