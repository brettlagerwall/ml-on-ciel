package canopy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.util.ArrayList;
import java.util.Random;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class CanopyReducer implements FirstClassJavaTask {

	private Reference[] mapperOutputs;
	private int numDimensions;
	private double t1;
	private double t2;

	public CanopyReducer(Reference[] mapperOutputs, int numDimensions,
		double t1, double t2) {
		this.mapperOutputs = mapperOutputs;
		this.numDimensions = numDimensions;
		this.t1 = t1;
		this.t2 = t2;
	}

	@Override
	public Reference[] getDependencies() {
		return mapperOutputs;
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Invoking reducer.");

		ArrayList<double[]> oldCentres = new ArrayList<double[]>();
		for (Reference mapOut: mapperOutputs) {
			ObjectInputStream inputStream = new ObjectInputStream
				(new FileInputStream(Ciel.RPC.getFilenameForReference
				(mapOut)));
			ArrayList<double[]> mapOutCentres =
				(ArrayList<double[]>) inputStream.readObject();
			oldCentres.addAll(mapOutCentres);
			inputStream.close();
		}

		//print(oldCentres, "/tmp/data/oldCentres.txt");


		Random rand = new Random();
		ArrayList<double[]> newCentres = new ArrayList<double[]>();
		ArrayList<double[]> marked = new ArrayList<double[]>();

		while (oldCentres.size() > 0) {
			// Find new centre and remove it from the relevant list.
			int newCentre =
				(int)(rand.nextDouble() * (oldCentres.size() + marked.size()));
			double[] centreArr;
			if (newCentre >= oldCentres.size()) {
				centreArr = marked.remove(newCentre - oldCentres.size());
			}
			else {
				centreArr = oldCentres.remove(newCentre);
			}

			// Add new centre to the list of centres.
			newCentres.add(centreArr);

			// Remove points which are within t1 distance of centre.
			// Remove points and add them to marked list if they are within t2
			// of centre.
			for (int i = 0; i < oldCentres.size(); i++) {
				if (getDistance(centreArr, oldCentres.get(i)) <= t1) {
					oldCentres.remove(i);
					i--;
				}
				else if (getDistance(centreArr, oldCentres.get(i)) <= t2) {
					marked.add(oldCentres.remove(i));
					i--;
				}
			}

			// Remove points from the marked list if they are within t1
			// distance of the centre.
			for (int i = 0; i < marked.size(); i++) {
				if (getDistance(centreArr, marked.get(i)) <= t1) {
					marked.remove(i);
					i--;
				}
			}	
		}

		//print(newCentres, "/tmp/data/finalCentres.txt");

		WritableReference finalOutputReference =
			Ciel.RPC.getNewObjectFilename("canopy_clusters");
		DataOutputStream outputStream = new DataOutputStream
			(new BufferedOutputStream
			(finalOutputReference.open(), 1048576));
			
		for (int i = 0; i < newCentres.size(); i++) {
			for (int j = 0; j < numDimensions; j++) {
				outputStream.writeDouble(newCentres.get(i)[j]);
			}
		}
		outputStream.close();

		Ciel.returnPlainString("Complete");

	}

	@Override
	public void setup() {
	}

	private double getDistance(double[] point1, double[] point2) {
		double distance = 0.0;
		for (int i = 0; i < point1.length; i++) {
			distance += (point1[i] - point2[i]) * (point1[i] - point2[i]);
		}
		return Math.sqrt(distance);
	}

	private void print(ArrayList<double[]> vectors, String path)
		throws Exception {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		for (int i = 0; i < vectors.size(); i++) {
			for (int j = 0; j < numDimensions; j++) {
				bw.write(vectors.get(i)[j] + " ");
			}
			bw.write("\n");
		}
		bw.close();
	}
}
