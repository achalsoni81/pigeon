package com.twitter.pig.backend.hadoop.executionegine.tez;
import org.apache.tez.dag.api.client.DAGStatus;

public interface DAGProgressNotificationListener {

	void dagStateUpdate(DAGStatus.State state);

}
