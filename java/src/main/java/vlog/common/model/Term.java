package vlog.common.model;

public abstract class Term {

	private final String name;

	public Term(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

}
