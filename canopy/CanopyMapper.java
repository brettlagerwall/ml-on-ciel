package canopy;

// Brett
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
//

import java.io.BufferedInputStream;
import java.io.DataInputStream;

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
		System.out.println("Got to the mapper" + idNum);

// Brett
		File file = new File("/tmp/data/" + idNum + ".txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
//

		/*DataInputStream inputStream = new DataInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(data)));*/
		
		DataInputStream inputStream = new DataInputStream
			(new BufferedInputStream((Ciel.RPC.getStreamForReference
			(data, 1048576, false, true, false)), 1048576));

		double[][] vectors = new double[numVectors][numDimensions];
		for (int i = 0; i < numVectors; i++) {
			for (int j = 0; j < numDimensions; j++) {
				vectors[i][j] = inputStream.readDouble();
			}
		}

// Brett
		for (int i = 0; i < numVectors; i++) {
			for (int j = 0; j < numDimensions; j++) {
				bw.write(vectors[i][j] + " ");
			}
			bw.write("\n");
		}
		bw.close();
//
	}

	@Override
	public void setup() {
	}
}
