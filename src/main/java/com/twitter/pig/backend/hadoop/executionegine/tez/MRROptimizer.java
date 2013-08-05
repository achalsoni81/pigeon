package com.twitter.pig.backend.hadoop.executionegine.tez;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
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
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;

public class MRROptimizer extends TezOpPlanVisitor {

	private Object inIllustrator;
	private Log log = LogFactory.getLog(getClass());


	public MRROptimizer(TezOperPlan plan) {
		super(plan, new ReverseDependencyOrderWalker<TezOperator, TezOperPlan>(plan));
		//this.inIllustrator = inIllustrator;
		log.info("MR plan size before optimization: " + plan.size());
	}


	@Override
	public void visit() throws VisitorException {
		super.visit();
		log.info("MR plan size after optimization: " + mPlan.size());
	}

	public void visitTezOp(TezOperator tezOper) throws VisitorException {


		if (tezOper instanceof MapOper) {

			List<TezOperator> predecessors = getPlan().getPredecessors(tezOper);

			if (predecessors != null && predecessors.size() == 1) {

				TezOperator predecessor = predecessors.get(0);
				LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(((ReduceOper) predecessor).getPlan(), POStore.class);
				if (stores.size() != 1) {
					return;
				}
				String predecessorStoreFile = stores.get(0).getSFile().getFileName();
				StoreFuncInterface predecessorStoreFunc = stores.get(0).getStoreFunc();
				if (!(predecessorStoreFunc instanceof InterStorage) || !(stores.get(0).isTmpStore())) {
					return;
				}
				
				if (!((MapOper) tezOper).getPlan().isEmpty()) {
					LinkedList<POLoad> loads = PlanHelper.getPhysicalOperators(((MapOper) tezOper).getPlan(), POLoad.class);
					if (loads.size() != 1) {
						return;
					}
					String loadFile = stores.get(0).getSFile().getFileName();
					StoreFuncInterface loadFunc = stores.get(0).getStoreFunc();
					if (!(loadFunc instanceof InterStorage) || !(loadFile.equals(predecessorStoreFile))) {
						return;
					}
				} 
				
				try {
					attachMapToReduce((MapOper) tezOper, (ReduceOper) predecessor);
				} catch (PlanException e) {}
				

			}

		}

		
	}
	
	
	private void attachMapToReduce(MapOper tezOper, ReduceOper predecessor) throws VisitorException, PlanException {
		
		
		predecessor.combinePlan = tezOper.combinePlan;
		predecessor.mapKeyType = tezOper.mapKeyType;
		predecessor.setNeedsDistinctCombiner(tezOper.needsDistinctCombiner());
		predecessor.setUseSecondaryKey(tezOper.getUseSecondaryKey());
		predecessor.setQuantFile(tezOper.getQuantFile());
		predecessor.setCustomPartitioner(tezOper.getCustomPartitioner());
		predecessor.setSkewedJoin(tezOper.isSkewedJoin());
		predecessor.setGlobalSort(tezOper.isGlobalSort());
		predecessor.setLimitAfterSort(tezOper.isLimitAfterSort());
		//predecessor.isUDFComparatorUsed = 
		predecessor.setSecondarySortOrder(tezOper.getSecondarySortOrder());
		predecessor.setSkewedJoinPartitionFile(tezOper.getSkewedJoinPartitionFile());
		predecessor.setSortOrder(tezOper.getSortOrder());
		predecessor.setUsingTypedComparator(tezOper.isUsingTypedComparator());
		predecessor.mapKeyType = tezOper.mapKeyType;
		predecessor.setIsIntermediateReducer(true);
		
		if (!tezOper.getPlan().isEmpty()) {
			predecessor.getPlan();
			PhysicalPlan mapPlan = tezOper.getPlan();
    		LinkedList<POLoad> loads = PlanHelper.getPhysicalOperators(mapPlan, POLoad.class);
    		mapPlan.remove(loads.get(0));
    		
    		
    		PhysicalPlan reducePlan = predecessor.getPlan();
    		LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(reducePlan, POStore.class);
    		reducePlan.remove(stores.get(0));
    		
    		PhysicalOperator root = mapPlan.getRoots().get(0);
    		reducePlan.addAsLeaf(root);
    		addAndConnectSuccessors(reducePlan, root,mapPlan.getSuccessors(root));
    		
		}
		
		getPlan().removeAndReconnectMultiSucc(tezOper);
	}

private void addAndConnectSuccessors(PhysicalPlan pp, PhysicalOperator root, List<PhysicalOperator> successors) throws PlanException {
	
	if (successors != null && successors.size() != 0) {
		pp.add(root);
		for (PhysicalOperator successor: successors) {
			pp.add(successor);
			pp.connect(root, successor);
			addAndConnectSuccessors(pp, successor, pp.getSuccessors(root));
		}
	}
}

}
