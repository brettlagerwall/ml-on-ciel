package canopy;

import datageneration.DataGenerator;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class CanopyInitTask implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		int numVectors = Integer.parseInt(Ciel.args[0]);
		int numDimensions = Integer.parseInt(Ciel.args[1]);
		int numMappers = Integer.parseInt(Ciel.args[2]);
		double t1 = Double.parseDouble(Ciel.args[3]);
		double t2 = Double.parseDouble(Ciel.args[4]);

		Reference data;
		if (Ciel.args.length > 5) {
			// Read in the data 
			data = null;
		}
		else {
			// Create the data
			data = Ciel.spawn(new DataGenerator(numVectors,
					numDimensions, 0), null, 1)[0];		
		}

		Reference[] mapperOutputs = new Reference[numMappers];
		for (int i = 0; i < numMappers; i++) {
			mapperOutputs[i] = Ciel.spawn(new CanopyMapper
				(data, numVectors, numDimensions, numMappers, t1, t2, i),
				null, 1)[0];
		}

		Ciel.tailSpawn(new CanopyReducer
			(mapperOutputs, numDimensions, t1, t2), null);

	}

	@Override
	public void setup() {
	}
}
