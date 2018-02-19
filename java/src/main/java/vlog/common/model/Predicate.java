package vlog.common.model;

public class Predicate {
	// FIXME: do we want to allow predicates with the same name and different number
	// of parameters?
	private final String name;

	public Predicate(final String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

}
