package net.acegik.jsondataflow;

import java.io.Serializable;

public class OpflowChangeEvent implements Serializable {
	private String action;
	private Object data;

	public OpflowChangeEvent() {
		super();
		this.action = null;
		this.data = null;
	}

	public OpflowChangeEvent(String action, Object data) {
		super();
		this.action = action;
		this.data = data;
	}

	public String getAction() {
		return action;
	}

	public OpflowChangeEvent setAction(String action) {
		this.action = action;
		return this;
	}

	public Object getData() {
		return data;
	}

	public OpflowChangeEvent setData(Object data) {
		this.data = data;
		return this;
	}
}
