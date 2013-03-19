package meanshift;

import datageneration.DataGenerator;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class MSInitTask implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		int numVectors = Integer.parseInt(Ciel.args[0]);
		int numDimensions = Integer.parseInt(Ciel.args[1]);

		Reference data;
		if (Ciel.args.length > 2) {
			// Read in the data 
			data = null;
		}
		else {
			// Create the data
			data = Ciel.spawn(new DataGenerator(numVectors,
					numDimensions, 0), null, 1)[0];

			Ciel.blockOn(data);		
		}

	}

	@Override
	public void setup() {
	}
}
