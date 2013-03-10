package canopy;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class CanopyMapper implements FirstClassJavaTask {

	private Reference data;
	private int numVectors;
	private int numDimensions;
	private int numMappers;
	private double t1;
	private double t2;
	private int idNum;

	public CanopyMapper(Reference data, int numVectors, int numDimensions,
		int numMappers, double t1, double t2, int idNum) {
		this.data = data;
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.numMappers = numMappers;
		this.t1 = t1;
		this.t2 = t2;
		this.idNum = idNum;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Got to the mapper" + idNum);
	}

	@Override
	public void setup() {
	}
}
