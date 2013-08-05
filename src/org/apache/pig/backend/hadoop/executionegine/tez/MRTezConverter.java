package org.apache.pig.backend.hadoop.executionegine.tez;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class MRTezConverter extends MROpPlanVisitor {

	private Object inIllustrator;
	private Log log = LogFactory.getLog(getClass());
	private TezOperPlan tezPlan;
	private Hashtable<MapReduceOper, TezOperator> mapOperToTezOper;
	private String scope;
	private NodeIdGenerator nig;

	public MRTezConverter(MROperPlan plan) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		//this.inIllustrator = inIllustrator;
		tezPlan = new TezOperPlan();
		mapOperToTezOper = new Hashtable<MapReduceOper, TezOperator>();
		scope = null;
		nig = NodeIdGenerator.getGenerator();
	}


	@Override
	public void visit() throws VisitorException {
		super.visit();
		log.info("MR plan size after optimization: " + mPlan.size());
	}

	public void visitMROp(MapReduceOper mr) throws VisitorException {

		try {

			if (scope == null) {
				scope = mr.getOperatorKey().getScope();
			}

			MapOper map = extractMapOper(mr);
			ReduceOper reduce = extractReduceOper(mr);


			tezPlan.add(map);
			if (reduce != null) {
				tezPlan.add(reduce);
				tezPlan.connect(map, reduce);
				mapOperToTezOper.put(mr, reduce);
				List<MapReduceOper> predecessors = getPlan().getPredecessors(mr);
				if (predecessors != null) {
					for (MapReduceOper mrPredecessor : getPlan().getPredecessors(mr)) {
						tezPlan.connect(mapOperToTezOper.get(mrPredecessor), map);
					}
				}
			} else {
				mapOperToTezOper.put(mr, map);
				List<MapReduceOper> predecessors = getPlan().getPredecessors(mr);
				if (predecessors != null) {
					for (MapReduceOper mrPredecessor : getPlan().getPredecessors(mr)) {
						tezPlan.connect(mapOperToTezOper.get(mrPredecessor), map);
					}
				}
			}
		} catch (PlanException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public TezOperPlan getTezPlan() {
		return tezPlan;
	}

	private MapOper extractMapOper(MapReduceOper mr) {
		// can't return null if empty map plan because of multiple inputs (could be a dummy mapper that is unfortunately necessary for sorting)
		MapOper map = new MapOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
		map.setPlan(mr.mapPlan);
		map.combinePlan = mr.combinePlan;
		map.mapKeyType = mr.mapKeyType;
		map.endOfAllInputInMap = mr.isEndOfAllInputSetInMap();
		map.needsDistinctCombiner = mr.needsDistinctCombiner();
		map.useSecondaryKey = mr.getUseSecondaryKey();
		map.quantFile = mr.getQuantFile();
		map.customPartitioner = mr.getCustomPartitioner();
		map.combineSmallSplits = mr.combineSmallSplits();
		//map.isMapOnly = mr.
		map.setSkewedJoin(mr.isSkewedJoin());
		map.setGlobalSort(mr.isGlobalSort());
		map.setLimitAfterSort(mr.isLimitAfterSort());
		//map.isUDFComparatorUsed = 
		map.setUDFs(mr.UDFs);
		map.setSecondarySortOrder(mr.getSecondarySortOrder());
		map.setSkewedJoinPartitionFile(mr.getSkewedJoinPartitionFile());
		map.setSortOrder(mr.getSortOrder());
		//map.setUsingTypedComparator(mr.usingTypedComparator);

		return map;
	}


	private ReduceOper extractReduceOper(MapReduceOper mr) {

		if (mr.reducePlan.isEmpty()) {
			return null;
		}

		ReduceOper reduce = new ReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
		reduce.setPlan(mr.reducePlan);
		reduce.setUDFs(mr.UDFs);
		reduce.setEndOfAllInputSetInReduce(mr.isEndOfAllInputSetInReduce());
		/*
		reduce.combinePlan = mr.combinePlan;
		reduce.mapKeyType = mr.mapKeyType;
		reduce.needsDistinctCombiner = mr.needsDistinctCombiner();
		reduce.useSecondaryKey = mr.getUseSecondaryKey();
		reduce.quantFile = mr.getQuantFile();
		reduce.customPartitioner = mr.getCustomPartitioner();
		//reduce.isMapOnly = mr.
		reduce.setSkewedJoin(mr.isSkewedJoin());
		reduce.setGlobalSort(mr.isGlobalSort());
		reduce.setLimitAfterSort(mr.isLimitAfterSort());
		//reduce.isUDFComparatorUsed = 
		reduce.setSecondarySortOrder(mr.getSecondarySortOrder());
		reduce.setSkewedJoinPartitionFile(mr.getSkewedJoinPartitionFile());
		reduce.setSortOrder(mr.getSortOrder());
		reduce.setUsingTypedComparator(mr.usingTypedComparator);

		 */

		return reduce;
	}
}
