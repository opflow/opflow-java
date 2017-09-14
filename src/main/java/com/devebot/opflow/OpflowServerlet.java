package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowServerlet {
    
    private OpflowPubsubHandler configurer;
    private OpflowRpcWorker rpcWorker;
    private OpflowPubsubHandler subscriber;
    
    private final ListenerMap listenerMap;
    
    private final Map<String, Object> kwargs;
    
    public OpflowServerlet(ListenerMap listeners, Map<String, Object> kwargs) throws OpflowBootstrapException {
        this.kwargs = kwargs;
        
        if (listeners == null) {
            throw new OpflowBootstrapException("Listener definitions must not be null");
        }
        listenerMap = listeners;
        
        Map<String, Object> configurerCfg = (Map<String, Object>)this.kwargs.get("configurer");
        Map<String, Object> rpcWorkerCfg = (Map<String, Object>)this.kwargs.get("rpcWorker");
        Map<String, Object> subscriberCfg = (Map<String, Object>)this.kwargs.get("subscriber");
        
        HashSet<String> checkExchange = new HashSet<String>();
        HashSet<String> checkQueue = new HashSet<String>();
        HashSet<String> checkRecyclebin = new HashSet<String>();
        
        if (configurerCfg != null) {
            if (configurerCfg.get("exchangeName") == null || configurerCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            } 
            if (!checkExchange.add(configurerCfg.get("exchangeName").toString() + configurerCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
            if (configurerCfg.get("subscriberName") != null && !checkQueue.add(configurerCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Configurer[subscriberName] must not be duplicated");
            }
            if (configurerCfg.get("recyclebinName") != null) checkRecyclebin.add(configurerCfg.get("recyclebinName").toString());
        }

        if (rpcWorkerCfg != null) {
            if (rpcWorkerCfg.get("exchangeName") == null || rpcWorkerCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid RpcWorker connection parameters");
            }
            if (!checkExchange.add(rpcWorkerCfg.get("exchangeName").toString() + rpcWorkerCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated RpcWorker connection parameters");
            }
            if (rpcWorkerCfg.get("operatorName") != null && !checkQueue.add(rpcWorkerCfg.get("operatorName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[operatorName] must not be duplicated");
            }
            if (rpcWorkerCfg.get("responseName") != null && !checkQueue.add(rpcWorkerCfg.get("responseName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[responseName] must not be duplicated");
            }
        }
        
        if (subscriberCfg != null) {
            if (subscriberCfg.get("exchangeName") == null || subscriberCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Subscriber connection parameters");
            }
            if (!checkExchange.add(subscriberCfg.get("exchangeName").toString() + subscriberCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated Subscriber connection parameters");
            }
            if (subscriberCfg.get("subscriberName") != null && !checkQueue.add(subscriberCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Subscriber[subscriberName] must not be duplicated");
            }
            if (subscriberCfg.get("recyclebinName") != null) checkRecyclebin.add(subscriberCfg.get("recyclebinName").toString());
        }
        
        checkRecyclebin.retainAll(checkQueue);
        if (!checkRecyclebin.isEmpty()) {
            throw new OpflowBootstrapException("Invalid recyclebinName (duplicated with some queueNames)");
        }
        
        try {
            if (configurerCfg != null && listenerMap.getConfigurer() != null) {
                configurer = new OpflowPubsubHandler(configurerCfg);
            }

            if (rpcWorkerCfg != null && 
                    listenerMap.getWorkerEntries() != null && 
                    listenerMap.getWorkerEntries().length > 0) {
                rpcWorker = new OpflowRpcWorker(rpcWorkerCfg);
            }

            if (subscriberCfg != null && listenerMap.getSubscriber() != null) {
                subscriber = new OpflowPubsubHandler(subscriberCfg);
            }
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
    }
    
    public final void start() {
        if (configurer != null) {
            configurer.subscribe(listenerMap.getConfigurer());
        }
        if (rpcWorker != null) {
            for(RpcWorkerEntry entry: listenerMap.getWorkerEntries()) {
                rpcWorker.process(entry.getRoutineId(), entry.getListener());
            }
        }
        if (subscriber != null) {
            subscriber.subscribe(listenerMap.getSubscriber());
        }
    }
    
    public final void close() {
        if (configurer != null) configurer.close();
        if (rpcWorker != null) rpcWorker.close();
        if (subscriber != null) subscriber.close();
    }
    
    public static class RpcWorkerEntry {
        private final String routineId;
        private final OpflowRpcListener listener;

        public RpcWorkerEntry(String routineId, OpflowRpcListener listener) {
            this.routineId = routineId;
            this.listener = listener;
        }

        public String getRoutineId() {
            return routineId;
        }

        public OpflowRpcListener getListener() {
            return listener;
        }
    }
    
    public static class ListenerMap {
        private final OpflowPubsubListener configurer;
        private final RpcWorkerEntry[] rpcWorkerEntries;
        private final OpflowPubsubListener subscriber;
        
        public ListenerMap(OpflowPubsubListener configurer, 
                RpcWorkerEntry[] rpcWorkerEntries, 
                OpflowPubsubListener subscriber) throws OpflowBootstrapException {
            if (configurer == null && subscriber == null &&
                    (rpcWorkerEntries == null || rpcWorkerEntries.length == 0)) {
                throw new OpflowBootstrapException("At least one listener must be not null");
            }
            this.configurer = configurer;
            if (rpcWorkerEntries != null) {
                this.rpcWorkerEntries = rpcWorkerEntries;
            } else {
                this.rpcWorkerEntries = new RpcWorkerEntry[0];
            }
            this.subscriber = subscriber;
        }
        
        public ListenerMap(OpflowPubsubListener configurer,
                RpcWorkerEntry[] rpcWorkerEntries) throws OpflowBootstrapException {
            this(configurer, rpcWorkerEntries, null);
            if (configurer == null) {
                throw new OpflowBootstrapException("Configurer should be defined");
            }
            if (rpcWorkerEntries == null || rpcWorkerEntries.length == 0) {
                throw new OpflowBootstrapException("RpcWorkers should be defined");
            }
        }
        
        public ListenerMap(OpflowPubsubListener configurer, 
                OpflowPubsubListener subscriber) throws OpflowBootstrapException {
            this(configurer, null, subscriber);
            if (configurer == null) {
                throw new OpflowBootstrapException("Configurer should be defined");
            }
            if (subscriber == null) {
                throw new OpflowBootstrapException("Subscriber should be defined");
            }
        }
        
        public OpflowPubsubListener getConfigurer() {
            return configurer;
        }

        public RpcWorkerEntry[] getWorkerEntries() {
            return rpcWorkerEntries.clone();
        }

        public OpflowPubsubListener getSubscriber() {
            return subscriber;
        }
    }
}
