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
    
    public static abstract class Filter {
        protected OpflowRestrictable.Filter parent = null;
        
        public void setParent(OpflowRestrictable.Filter parent) {
            this.parent = parent;
        }
        
        protected <T> T execute(Action<T> action) throws Throwable {
            T result;
            if (this.parent != null) {
                result = this.parent.filter(action);
            } else {
                result = action.process();
            }
            return result;
        }
        
        abstract public <T> T filter(Action<T> action) throws Throwable;
    }
    
    public static class Runner extends OpflowRestrictable.Filter {
        protected final List<Filter> chain = new LinkedList<>();

        protected void append(Filter filter) {
            if (filter != null) {
                this.chain.add(0, filter);
            }
        }

        protected void append(List<Filter> list) {
            if (list != null) {
                for (Filter filter : list) {
                    this.append(filter);
                }
            }
        }

        @Override
        public <T> T filter(Action<T> action) throws Throwable {
            Filter entrypoint = parent;
            for (Filter node : chain) {
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
