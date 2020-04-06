package com.devebot.opflow.services;

import com.devebot.opflow.OpflowConstant;
import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowPromMeasurer;
import com.devebot.opflow.OpflowRestrictable;
import com.devebot.opflow.OpflowRestrictor;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cuoi
 */
public class OpflowRestrictorMaster extends OpflowRestrictable.Runner implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRestrictorMaster.class);

    protected final String componentId;
    protected final OpflowLogTracer logTracer;
    protected final OpflowPromMeasurer measurer;

    private final OpflowRestrictor.OnOff onoffRestrictor;
    private final OpflowRestrictor.Valve valveRestrictor;
    private final OpflowRestrictor.Pause pauseRestrictor;
    private final OpflowRestrictor.Limit limitRestrictor;

    public OpflowRestrictorMaster(Map<String, Object> options) {
        options = OpflowObjectTree.ensureNonNull(options);

        componentId = OpflowUtil.getStringField(options, OpflowConstant.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) options.get(OpflowConstant.COMP_MEASURER);
        logTracer = OpflowLogTracer.ROOT.branch("restrictorId", componentId);

        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("Restrictor[${restrictorId}].new()")
                .stringify());

        onoffRestrictor = new OpflowRestrictor.OnOff(options);
        valveRestrictor = new OpflowRestrictor.Valve(options);
        pauseRestrictor = new OpflowRestrictor.Pause(options);
        limitRestrictor = new OpflowRestrictor.Limit(options);

        super.append(onoffRestrictor.setLogTracer(logTracer));
        super.append(valveRestrictor.setLogTracer(logTracer).setMeasurer(measurer));
        super.append(pauseRestrictor.setLogTracer(logTracer));
        super.append(limitRestrictor.setLogTracer(logTracer));

        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("Restrictor[${restrictorId}].new() end!")
                .stringify());
    }

    public String getComponentId() {
        return componentId;
    }

    public OpflowRestrictor.Valve getValveRestrictor() {
        return valveRestrictor;
    }

    public boolean isActive() {
        return onoffRestrictor.isActive();
    }

    public void setActive(boolean enabled) {
        onoffRestrictor.setActive(enabled);
    }

    public boolean isBlocked() {
        return valveRestrictor.isBlocked();
    }

    public void block() {
        if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(logTracer
                .text("Restrictor[${restrictorId}].block()")
                .stringify());
        valveRestrictor.block();
        if (logTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) LOG.trace(logTracer
                .text("Restrictor[${restrictorId}].block() end!")
                .stringify());
    }

    public void unblock() {
        valveRestrictor.unblock();
    }

    public boolean isPauseEnabled() {
        return pauseRestrictor.isPauseEnabled();
    }

    public long getPauseTimeout() {
        return pauseRestrictor.getPauseTimeout();
    }

    public long getPauseDuration() {
        return pauseRestrictor.getPauseDuration();
    }

    public long getPauseElapsed() {
        return pauseRestrictor.getPauseElapsed();
    }

    public boolean isPaused() {
        return pauseRestrictor.isPaused();
    }

    public Map<String, Object> pause(long duration) {
        return pauseRestrictor.pause(duration);
    }

    public Map<String, Object> unpause() {
        return pauseRestrictor.unpause();
    }

    public int getSemaphoreLimit() {
        return limitRestrictor.getSemaphoreLimit();
    }

    public int getSemaphorePermits() {
        return limitRestrictor.getSemaphorePermits();
    }

    public boolean isSemaphoreEnabled() {
        return limitRestrictor.isSemaphoreEnabled();
    }

    public long getSemaphoreTimeout() {
        return limitRestrictor.getSemaphoreTimeout();
    }

    @Override
    public void close() {
        pauseRestrictor.close();
    }
}