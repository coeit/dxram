package de.hhu.bsinfo.dxram.function;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

@FunctionalInterface
public interface DistributableFunction extends Serializable {
    void execute(final DXRAMServiceAccessor p_serviceAccessor);
}
