package meanshift;

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedInputStream;
import java.io.DataInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class MSInitClusters implements FirstClassJavaTask {

	private Reference data;
	private int numVectors;
	private int numDimensions;
	private int numMappers;
	private int idNum;

	public MSInitClusters(Reference data, int numVectors, int numDimensions,
		int numMappers, int idNum) {
		this.data = data;
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.numMappers = numMappers;
		this.idNum = idNum;
	}

	@Override
	public Reference[] getDependencies() {
		return new Reference[] {data};
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Initializing Clusters");
	
		DataInputStream inputStream = new DataInputStream
			(new BufferedInputStream((Ciel.RPC.getStreamForReference
			(data, 1048576, false, true, false)), 1048576));

		int vectorsPerMapper =
			(int)(Math.ceil((double)numVectors / numMappers));

		List<MSCluster> clusters = new ArrayList<MSCluster>();

		for (int i = 0; i < numVectors; i++) {
			if (i >= vectorsPerMapper * idNum &&
				i < vectorsPerMapper * (idNum + 1)) {

				MSCluster c = new MSCluster(numDimensions);
				double[] point = new double[numDimensions];
				for (int j = 0; j < numDimensions; j++) {
					point[j] = inputStream.readDouble();
				}
				c.add(point);
				clusters.add(c);
			}
			else {
				for (int j = 0; j < numDimensions; j++) {
					inputStream.readDouble();
				}
			}
		}

		Ciel.returnObject(clusters);
	}

	@Override
	public void setup() {
	}
}
