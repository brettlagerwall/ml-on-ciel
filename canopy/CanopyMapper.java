package canopy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.util.Random;
import java.util.ArrayList;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class CanopyMapper implements FirstClassJavaTask {

	private Reference data;
	private int numVectors;
	private int numDimensions;
	private int numMappers;
	private double t1;
	private double t2;
	private int idNum;

	public CanopyMapper(Reference data, int numVectors, int numDimensions,
		int numMappers, double t1, double t2, int idNum) {
		this.data = data;
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.numMappers = numMappers;
		this.t1 = t1;
		this.t2 = t2;
		this.idNum = idNum;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] {data};
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Mapper id: " + idNum);
	
		DataInputStream inputStream = new DataInputStream
			(new BufferedInputStream((Ciel.RPC.getStreamForReference
			(data, 1048576, false, true, false)), 1048576));

		int vectorsPerMapper =
			(int)(Math.ceil((double)numVectors / numMappers));
		ArrayList<double[]> vectors = new ArrayList<double[]>();

		for (int i = 0; i < numVectors; i++) {
			if (i >= vectorsPerMapper * idNum &&
				i < vectorsPerMapper * (idNum + 1)) {

				double[] tempArr = new double[numDimensions];
				for (int j = 0; j < numDimensions; j++) {
					tempArr[j] = inputStream.readDouble();
				}
				vectors.add(tempArr);
			}
			else {
				for (int j = 0; j < numDimensions; j++) {
					inputStream.readDouble();
				}
			}
		}

		print(vectors, "/tmp/data/vectors" + idNum + ".txt");

		Random rand = new Random(idNum);
		ArrayList<double[]> centres = new ArrayList<double[]>();
		ArrayList<double[]> marked = new ArrayList<double[]>();

		while (vectors.size() > 0) {
			// Find new centre and remove it from the relevant list.
			int newCentre =
				(int)(rand.nextDouble() * (vectors.size() + marked.size()));
			double[] centreArr;
			if (newCentre >= vectors.size()) {
				centreArr = marked.remove(newCentre - vectors.size());
			}
			else {
				centreArr = vectors.remove(newCentre);
			}

			// Add new centre to the list of centres.
			centres.add(centreArr);

			// Remove points which are within t1 distance of centre.
			// Remove points and add them to marked list if they are within t2
			// of centre.
			for (int i = 0; i < vectors.size(); i++) {
				if (getDistance(centreArr, vectors.get(i)) <= t1) {
					vectors.remove(i);
					i--;
				}
				else if (getDistance(centreArr, vectors.get(i)) <= t2) {
					marked.add(vectors.remove(i));
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

		print(centres, "/tmp/data/centres" + idNum + ".txt");

		Ciel.returnObject(centres);
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
