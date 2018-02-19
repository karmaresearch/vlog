package vlog.common;

public class HaltingStrategy {

	private final HaltingStrategyEnum haltingStrategyEnum;
	private long haltingLimit;

	private HaltingStrategy(HaltingStrategyEnum haltingStrategyEnum, long haltingLimit) {
		super();
		this.haltingStrategyEnum = haltingStrategyEnum;
		this.haltingLimit = haltingLimit;
	}
	
	public HaltingStrategy TimeoutHaltingStrategy(long haltAfterMiliseconds) {
		return new HaltingStrategy(HaltingStrategyEnum.TIMEOUT_MILISECONDS, haltAfterMiliseconds);
	}
	
	public HaltingStrategy MaxFactsHaltingStrategy(long haltAfterMaxFacts) {
		return new HaltingStrategy(HaltingStrategyEnum.MAX_GENERATED_FACTS, haltAfterMaxFacts);
	}

	public long getHaltingLimit() {
		return haltingLimit;
	}

	public void setHaltingLimit(long haltingLimit) {
		this.haltingLimit = haltingLimit;
	}

	public HaltingStrategyEnum getHaltingStrategyEnum() {
		return haltingStrategyEnum;
	}

}
