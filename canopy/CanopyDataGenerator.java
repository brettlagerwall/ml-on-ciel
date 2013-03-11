package canopy;

import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.util.Random;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class CanopyDataGenerator implements FirstClassJavaTask {

	private int numVectors;
	private int numDimensions;
	private int seed;
	
	public CanopyDataGenerator(int numVectors, int numDimensions, int seed) {
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.seed = seed;
	}

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {

		double minValue = 0.0;
		double maxValue = 1000000.0;
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		
		DataOutputStream dos = new DataOutputStream(
			new BufferedOutputStream(out.open(), 1048576));
		
		Random rand = new Random(seed);


		for (int i = 0; i < numVectors; i++) {
			for (int j = 0; j < numDimensions; j++) {
				dos.writeDouble(rand.nextDouble() * (maxValue - minValue) + 						minValue);
			}
		}
		dos.close();

	}

	@Override
	public void setup() {
	}

}
