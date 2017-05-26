package net.acegik.jsondataflow;

import java.io.Serializable;

public class FlowChangeEvent implements Serializable {
	private String action;
	private Object data;

	public FlowChangeEvent() {
		super();
		this.action = null;
		this.data = null;
	}

	public FlowChangeEvent(String action, Object data) {
		super();
		this.action = action;
		this.data = data;
	}

	public String getAction() {
		return action;
	}

	public FlowChangeEvent setAction(String action) {
		this.action = action;
		return this;
	}

	public Object getData() {
		return data;
	}

	public FlowChangeEvent setData(Object data) {
		this.data = data;
		return this;
	}
}
