package datageneration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.util.Random;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class DataGenerator implements FirstClassJavaTask {

	private int numVectors;
	private int numDimensions;
	private int seed;
	
	public DataGenerator(int numVectors, int numDimensions, int seed) {
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

		/*File file = new File("/tmp/data/data.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);*/

		double minValue = 0.0;
		double maxValue = 1000000.0;
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		
		DataOutputStream dos = new DataOutputStream(
			new BufferedOutputStream(out.open(), 1048576));
		
		// The seed was only used for test purposes.
		Random rand = new Random();


		for (int i = 0; i < numVectors; i++) {
			for (int j = 0; j < numDimensions; j++) {
				double d = rand.nextDouble() * (maxValue - minValue)
					+ minValue;
				dos.writeDouble(d);
				//bw.write(d + " ");
			}
			//bw.write("\n");
		}
		dos.close();
		//bw.close();
	}

	@Override
	public void setup() {
	}

}
