package com.devebot.opflow;

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
        
        protected T execute(Action<T> action) throws Throwable {
            T result;
            if (this.parent != null) {
                result = this.parent.filter(action);
            } else {
                result = action.process();
            }
            return result;
        }
        
        abstract public T filter(Action<T> action) throws Throwable;
    }
    
    public static class Runner<T> extends OpflowRestrictable.Filter<T> {
        protected final List<Filter<T>> chain = new LinkedList<>();

        protected void append(Filter<T> filter) {
            if (filter != null) {
                this.chain.add(0, filter);
            }
        }

        protected void append(List<Filter<T>> list) {
            if (list != null) {
                for (Filter<T> filter : list) {
                    this.append(filter);
                }
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
