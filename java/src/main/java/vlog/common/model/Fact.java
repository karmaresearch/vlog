package vlog.common.model;

public class Fact extends Atom {

	public Fact(Predicate predicate, Constant[] arguments) {
		super(predicate, arguments);
	}
	
	public Constant[] getArguments() {
		return (Constant[]) super.getArguments();
	}

}
