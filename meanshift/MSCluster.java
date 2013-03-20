package meanshift;

import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;

public class MSCluster implements Serializable {

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

	/*
	Adds the point to the canopy if the distance between the point and the
	old centre is less than the threshold.
	*/
	public void add(double[] point, double[] oldCentre, double threshold) {
		if (getDistance(point, oldCentre) < threshold) {		
			boundPoints.add(point);
			count++;
			updateNecessary = true;
		}
	}

	public double[] getCentre() {
		if (updateNecessary) {
			calculateCentre();
		}

		return centre;
	}

	public void clear() {
		boundPoints = new ArrayList<double[]>();
		count = 0;
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

	private double getDistance(double[] p1, double[] p2) {
		double sum = 0.0;
		for (int i = 0; i < p1.length; i++) {
			sum += (p1[i] - p2[i]) * (p1[i] - p2[i]);
		}
		return Math.sqrt(sum);
	}
	
}
