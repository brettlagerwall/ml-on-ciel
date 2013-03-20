package meanshift;

import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;

public class MSCluster implements Serializable {

	private List<double[]> boundPoints = new ArrayList<double[]>();
	private int count = 0;
	private double[] centre;

	public MSCluster(int numDimensions) {
		centre = new double[numDimensions];
	}

	public void add(double[] point) {
		boundPoints.add(point);
		count++;
	}

	/*
	Adds the point to the canopy if the distance between the point and the
	old centre is less than the threshold.
	*/
	public void add(double[] point, double[] oldCentre, double threshold) {
		if (getDistance(point, oldCentre) < threshold) {		
			boundPoints.add(point);
			count++;
		}
	}

	public double[] getCentre() {
		return centre;
	}

	public void forceUpdateCentre() {
		calculateCentre();
	}

	public void clear() {
		boundPoints = new ArrayList<double[]>();
		count = 0;
	}

	public List<double[]> getBoundPoints() {
		return boundPoints;
	}

	public void merge(MSCluster other) {
		boundPoints.addAll(other.boundPoints);
		count += other.count;
	}

	public double getDistanceToCentre(double[] p) {
		return getDistance(p, centre);
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
	}

	private double getDistance(double[] p1, double[] p2) {
		double sum = 0.0;
		for (int i = 0; i < p1.length; i++) {
			sum += (p1[i] - p2[i]) * (p1[i] - p2[i]);
		}
		return Math.sqrt(sum);
	}
	
}
