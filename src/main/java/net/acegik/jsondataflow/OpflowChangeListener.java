package net.acegik.jsondataflow;

public interface OpflowChangeListener {
	public void objectReceived(OpflowChangeEvent event, OpflowChangeFeedback feedback);
}
