package com.devebot.opflow;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author pnhung177
 */
public class OpflowRestrictable {
    
    public interface Action<T> {
        public T process() throws Throwable;
    }
    
    public static abstract class Filter<T> {
        protected OpflowRestrictable.Filter<T> parent = null;
        
        public void setParent(OpflowRestrictable.Filter<T> parent) {
            this.parent = parent;
        }
        
        abstract public T filter(Action<T> action) throws Throwable;
    }
    
    public static class Runner<T> extends OpflowRestrictable.Filter<T> {
        private final List<Filter<T>> chain = new LinkedList<>();
        
        public Runner(List<Filter<T>> chain) {
            if (chain != null) {
                this.chain.addAll(chain);
                Collections.reverse(this.chain);
            }
        }
        
        @Override
        public T filter(Action<T> action) throws Throwable {
            Filter<T> entrypoint = parent;
            for (Filter<T> node : chain) {
                node.setParent(entrypoint);
                entrypoint = node;
            }
            if (entrypoint == null) {
                return null;
            }
            return entrypoint.filter(action);
        }
    }
}
