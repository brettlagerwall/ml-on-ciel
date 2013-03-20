package meanshift;

import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;

public class MSCluster {

	private List<double[]> boundPoints = new ArrayList<double[]>();
	private int count = 0;
	private double[] centre;
	private boolean updateNecessary = false;

	public MSCluster(int numDimensions) {
		centre = new double[numDimensions];
	}

	public void add(double[] point) {
		boundPoints.add(point);
		count++;
		updateNecessary = true;
	}

	public double[] getCentre() {
		if (updateNecessary) {
			calculateCentre();
		}

		return centre;
	}

	private void calculateCentre() {
		if (count > 0) {
			for (int i = 0; i < centre.length; i++) {
				double sum = 0.0;
				for (int j = 0; j < count; j++) {
					sum += boundPoints.get(j)[i];
				}
				centre[i] = sum / count;
			}
		}
		updateNecessary = false;
	}
	
}
