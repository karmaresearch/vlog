package vlog.common;


import java.io.Serializable;

import vlog.common.ChaseType;

/**
 * Contains information about the finished materialization process.
 * 
 * @author Irina Dragoste
 *
 */
public class MaterializationResult implements Serializable {

	private static final long serialVersionUID = 2539158477648782141L;

	/**
	 * true, if materialization terminated and was not halted by a HaltingStrategy
	 */
	private boolean complete;

	/**
	 * number of facts before the materialization, that serve as materialization
	 * input
	 */
	private long countEDBs;

	/**
	 * number of facts derrived from the materialization, that did not exist before
	 * the materialization
	 */
	private long countIDBs;

	/**
	 * total number of facts obtained after materialization, (edbs + idbs)
	 * 
	 */
	// FIXME are <tarantino> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
	// <director> .
	// and TE(<tarantino>,rdf:type,<director>)
	// considered different facst and counted twice?

	private long countFacts;

	/**
	 * duration of the materialization procedure (in miliseconds)
	 */
	private long durationInMilis;

	/**
	 * size of RAM used during materialization procedure (in bytes)
	 */
	// TODO better define this
	private long usedMemory;

	/**
	 * number of materialization iterations
	 */
	// TODO define how we count them for the restricted chase (if a rule and
	// substitution is blocked, is this an iteration?)
	private long countIterations;

	// ---------------------------------------------- THE FOLLOWING ARE OPTIONAL,
	// CAN BE USEFUL
	private long countUnnamedIndividuals;
	private long maxTermDepth;
	private long maxCycleDepth;
	/**
	 * counts the number of times a rule application was blocked during the
	 * restricted chase ({@link ChaseType#RESTRICTED}). <br>
	 * 0 in the case of ({@link ChaseType#OBLIVIOUS})
	 */
	private long countBlockedRuleApplications;

	public boolean isComplete() {
		return complete;
	}

	public void setComplete(boolean complete) {
		this.complete = complete;
	}

	public long getCountEDBs() {
		return countEDBs;
	}

	public void setCountEDBs(long countEDBs) {
		this.countEDBs = countEDBs;
	}

	public long getCountIDBs() {
		return countIDBs;
	}

	public void setCountIDBs(long countIDBs) {
		this.countIDBs = countIDBs;
	}

	public long getCountFacts() {
		return countFacts;
	}

	public void setCountFacts(long countFacts) {
		this.countFacts = countFacts;
	}

	public long getDurationInMilis() {
		return durationInMilis;
	}

	public void setDurationInMilis(long durationInMilis) {
		this.durationInMilis = durationInMilis;
	}

	public long getUsedMemory() {
		return usedMemory;
	}

	public void setUsedMemory(long usedMemory) {
		this.usedMemory = usedMemory;
	}

	public long getCountIterations() {
		return countIterations;
	}

	public void setCountIterations(long countIterations) {
		this.countIterations = countIterations;
	}

	public long getCountUnnamedIndividuals() {
		return countUnnamedIndividuals;
	}

	public void setCountUnnamedIndividuals(long countUnnamedIndividuals) {
		this.countUnnamedIndividuals = countUnnamedIndividuals;
	}

	public long getMaxTermDepth() {
		return maxTermDepth;
	}

	public void setMaxTermDepth(long maxTermDepth) {
		this.maxTermDepth = maxTermDepth;
	}

	public long getMaxCycleDepth() {
		return maxCycleDepth;
	}

	public void setMaxCycleDepth(long maxCycleDepth) {
		this.maxCycleDepth = maxCycleDepth;
	}

	public long getCountBlockedRuleApplications() {
		return countBlockedRuleApplications;
	}

	public void setCountBlockedRuleApplications(long countBlockedRuleApplications) {
		this.countBlockedRuleApplications = countBlockedRuleApplications;
	}

}
