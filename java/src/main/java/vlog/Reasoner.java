package vlog;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import vlog.common.ChaseType;
import vlog.common.HaltingStrategy;
import vlog.common.MaterializationResult;
import vlog.common.model.Fact;
import vlog.common.model.Rule;
import vlog.common.model.Predicate;

public class Reasoner {

	private Set<File> ruleFiles = new HashSet<File>();
	
	private Set<Rule> hornRules=new HashSet<Rule>();

	private ChaseType chaseType = ChaseType.OBLIVIOUS;

	/**
	 * If null, the materialization will halt when no new facts are produced.
	 */
	private HaltingStrategy haltingStrategy;

	public Reasoner() {
		super();
	}

	public Set<File> getRuleFiles() {
		return ruleFiles;
	}

	public void addRuleFiles(Collection<File> ruleFiles) {
		ruleFiles.addAll(ruleFiles);
	}

	public void clearRuleFiles() {
		ruleFiles.clear();
	}

	/**
	 * Materializes the facts in the facts database with the rules in the rule
	 * files, using the chase procedure. <br>
	 * The facts generated through materialization will not be stored into the facts
	 * database until updateFactsDBWithMaterializeResult is explicitly called. This
	 * implies that, between two successive materialize calls, the facts generated
	 * by the first materialize call will be discarded if the database is not
	 * explicitly updated. <br>
	 * Throws an exception if the given database has not been created.
	 *
	 * @return MaterializationResult containing information about the
	 *         materialization process
	 */
	public MaterializationResult materialize() {
		// TODO call VLog
		// send VLog all the rules in the rule files
		// send VLog the chase type, halting strategy and limit
		return null;
	}

	/**
	 * Stores the materialized sets of facts (obtained after the materialize method
	 * call) into the facts database.
	 *
	 * @return true, if new facts have been added to the facts DB
	 */
	public boolean updateFactsDBWithMaterializeResult() {
		clearMaterializedFacts();
		// TODO Vlog
		// if materialize() has not been called yet, do nothing and return false
		return false;
	}

	/**
	 * Removes all materialized facts. The information in the database is not
	 * modified when calling this method.
	 *
	 * @return true, if some facts were indeed cleared
	 */
	public boolean clearMaterializedFacts() {
		return false;
	}

	/**
	 * Exports the facts resulting from the materialize method call to given file in
	 * .nt format. <br>
	 * If the facts in factsDB have not been materialized yet (materialization has
	 * not been called yet), it exports the content of factsDB. <br>
	 * Throws and exception if factsDB database has not been created yet. <br>
	 * If a file with outputFile name already exists, it overwrites its content.
	 *
	 * @param outputFile
	 */
	// TODO created an empty database there
	public void exportMaterializeResult(File outputFile) {
		// TODO Vlog
		// TODO if materialize() has not been called yet, should we write an empty
		// file,
		// or not create the file at all?
	}

	/**
	 * Exports the facts resulting from the materialize method call to given
	 * database. <br>
	 * If the facts in factsDB have not been materialized yet (materialization has
	 * not been called yet), it exports the content of factsDB. <br>
	 * Throws and exception if factsDB database has not been created yet. <br>
	 * Throws and exception if outputFactsDB database is not empty. <br>
	 * 
	 * @param outputFactsDB
	 */
	public void exportMaterializeResult() {

	}

	/**
	 * Queries the EDB plus, if they have been materialized, the IDBs.
	 * 
	 * @param query
	 *            String representation of a query
	 * @return an iterator for the query answers
	 */
	public QueryIterator query(String query) {
		// TODO if querying is done on the facts in memory, then
		// ansswerQuery(program.getFacts)
		return null;
	}

	/**
	 * Queries for a given predicate the EDB , if they have been materialized, the
	 * IDBs.
	 * 
	 * @param predicate
	 * @return
	 */
	public Set<Fact> queryFactsForPredicate(Predicate predicate) {
		// TODO VLOG
		return null;
	}

	public long countIDBs() {
		return 0;
	}

	public long countEDBs() {
		return 0;
	}

	/**
	 * If the factsDB has not been materialized, it counts the number of facts in
	 * the factsDB. If it has been materialized, it counts the total number of
	 * generated facts.
	 *
	 * @return the number of facts in the current state of the chase.
	 */
	public long countFacts() {
		return countIDBs() + countEDBs();
	}

	public ChaseType getChaseType() {
		return chaseType;
	}

	public void setChaseType(ChaseType chaseType) {
		this.chaseType = chaseType;
	}

	public HaltingStrategy getHaltingStrategy() {
		return this.haltingStrategy;
	}

	public void setHaltingStrategy(HaltingStrategy haltingStrategy) {
		this.haltingStrategy = haltingStrategy;
	}
}
