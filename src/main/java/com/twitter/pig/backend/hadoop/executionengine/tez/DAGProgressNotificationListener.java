package com.twitter.pig.backend.hadoop.executionengine.tez;
import org.apache.tez.dag.api.client.DAGStatus;

public interface DAGProgressNotificationListener {

	void dagStateUpdate(DAGStatus.State state);

}
