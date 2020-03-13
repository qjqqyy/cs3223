/*
  assume that the first line of the file contain the names of the
  attributes of the relation. each subsequent line represents 1
  tuple of the relation. also assume that the fields of each line
  is delimited by tabs ("\t")
*/

import qp.utils.Attribute;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class ConvertTxtToTbl {

    public static void main(String[] args) throws IOException {
        // check the arguments
        if (args.length != 1) {
            System.out.println("usage: java ConvertTxtToTbl <tablename> \n creats <tablename>.tbl files");
            System.exit(1);
        }
        String tblname = args[0];
        String mdfile = tblname + ".md";
        String tblfile = tblname + ".tbl";

        /** open the input and output streams **/
        BufferedReader in = new BufferedReader(new FileReader(tblname + ".txt"));
        ObjectOutputStream outtbl = new ObjectOutputStream(new FileOutputStream(tblfile));

        /** First Line is METADATA **/
        int linenum = 0;
        String line;
        Schema schema = null;
        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(mdfile));
            schema = (Schema) ins.readObject();
        } catch (ClassNotFoundException ce) {
            System.out.println("class not found exception --- error in schema object file");
            System.exit(1);
        }

        boolean flag = false;
        StringTokenizer tokenizer;
        while ((line = in.readLine()) != null) {
            linenum++;
            tokenizer = new StringTokenizer(line);

            ArrayList<Object> data = new ArrayList<>();
            int attrIndex = 0;

            while (tokenizer.hasMoreElements()) {
                String dataElement = tokenizer.nextToken();
                int datatype = schema.typeOf(attrIndex);
                if (datatype == Attribute.INT) {
                    data.add(Integer.valueOf(dataElement));
                } else if (datatype == Attribute.REAL) {
                    data.add(Float.valueOf(dataElement));
                } else if (datatype == Attribute.STRING) {
                    data.add(dataElement);
                } else {
                    System.err.println("Invalid data type");
                    System.exit(1);
                }
                attrIndex++;
            }
            Tuple tuple = new Tuple(data);
            outtbl.writeObject(tuple);
        }
        outtbl.close();
        in.close();
    }

}
