package org.apache.pig.backend.hadoop.executionegine.tez;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionegine.tez.MapOper;
import org.apache.pig.backend.hadoop.executionegine.tez.ReduceOper;
import org.apache.pig.backend.hadoop.executionegine.tez.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionegine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionegine.tez.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;

public class TezEdgeAnnotator extends TezOpPlanVisitor {

	private Object inIllustrator;
	private Log log = LogFactory.getLog(getClass());


	public TezEdgeAnnotator(TezOperPlan plan) {
		super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
		//this.inIllustrator = inIllustrator;
		log.info("MR plan size before optimization: " + plan.size());
	}


	@Override
	public void visit() throws VisitorException {
		super.visit();
		log.info("MR plan size after optimization: " + mPlan.size());
	}

	public void visitTezOp(TezOperator tezOper) throws VisitorException {

		TezOperPlan tezPlan =  getPlan();
		List<TezOperator> successors = tezPlan.getSuccessors(tezOper);
		if (successors != null) {
			for (TezOperator successor : successors) {
				EdgeProperty edge = null;
				if (successor instanceof ReduceOper) {
					edge = new EdgeProperty(
							ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor(
									OnFileSortedOutput.class.getName(), null), new InputDescriptor(
											ShuffledMergedInput.class.getName(), null));
				}	else {
					edge =  tezPlan.new DependencyEdge();
				}
				tezPlan.annotateEdge(tezOper, successor, edge);
			}
		}
	}



}
