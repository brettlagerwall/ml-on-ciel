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
		int numMappers = Integer.parseInt(Ciel.args[2]);
		double t1 = Double.parseDouble(Ciel.args[3]);
		double t2 = Double.parseDouble(Ciel.args[4]);
		double delta = Double.parseDouble(Ciel.args[5]);

		Reference data;
		if (Ciel.args.length > 6) {
			// Read in the data 
			data = null;
		}
		else {
			// Create the data
			data = Ciel.spawn(new DataGenerator(numVectors,
					numDimensions, 0), null, 1)[0];
		}

		/*
		Generating initial clusters and splitting them into sets which each of
		the mappers can handle separately.
		*/
		Reference[] clusters = new Reference[numMappers];
		for (int i = 0; i < numMappers; i++) {
			clusters[i] = Ciel.spawn(new MSInitClusters
				(data, numVectors, numDimensions, numMappers, i, delta),
				null, 1)[0];
		}

		System.out.println("Finished initializing clusters");

		Reference[] mapperOutputs = new Reference[numMappers];
		for (int i = 0; i < numMappers; i++) {
			mapperOutputs[i] = Ciel.spawn(new MSMapper
				(clusters[i], numDimensions,
				t1, t2, i, 1), null, 1)[0];
		}

		Ciel.tailSpawn(new MSReducer(mapperOutputs, numDimensions,
			numMappers, t1, t2, 1), null);
	}

	@Override
	public void setup() {
	}
}
