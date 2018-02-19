package vlog.common.model;

public class Atom {
	
	private final Predicate predicate;
	private final Term[] arguments;
	
//	TODO how to make elements in the term array immutable
	
	public Atom(Predicate predicate, Term... arguments) {
		super();
		this.predicate = predicate;
		this.arguments = arguments;
	}
	
	public int arity() {
		return arguments.length;
	}

	public Predicate getPredicate() {
		return predicate;
	}

	public Term[] getArguments() {
		return arguments;
	}

}
