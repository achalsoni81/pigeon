package org.apache.pig.backend.hadoop.executionegine.tez;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.engine.api.Processor;

public abstract class TezOperator extends Operator<TezOpPlanVisitor> {


	public TezOperator(OperatorKey k) {
		super(k);
		// TODO Auto-generated constructor stub
	}

	public abstract  String getProcessor();

	public abstract TezConfiguration configure(PigContext pigContext, Configuration conf);

	public abstract int getParallelism();
	
	public abstract void configureVertex(Vertex operVertex, Configuration operConf,
			Map<String, LocalResource> commonLocalResources, Path remoteStagingDir);


	public void visit(TezOpPlanVisitor v) throws VisitorException {
		v.visitTezOp(this);
	}


	public boolean supportsMultipleInputs() {
		return true;
	}
	public boolean supportsMultipleOutputs() {
		return true;
	}

	public String name() {
		return "Tez - " + mKey.toString();
	}

}
