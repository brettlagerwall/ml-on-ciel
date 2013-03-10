package canopy;

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
		double t1 = Double.parseDouble(Ciel.args[3]);
		double t2 = Double.parseDouble(Ciel.args[4]);

		Reference data;
		if (Ciel.args.length > 4) {
			// Read in the data 
		}
		else {
			// Create the data
			data = Ciel.spawn(new CanopyDataGenerator(numVectors,
					numDimensions, 0), null, 1)[0];
			Ciel.blockOn(data);
		}

	}

	@Override
	public void setup() {
	}
}
