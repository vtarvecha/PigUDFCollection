package com.tsquare.pigudfcollection;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TupleToString extends EvalFunc<String> {

	private final String flag;

	public TupleToString(String flag) {
		this.flag = String.valueOf(flag);
	}

	@Override
	public String exec(Tuple input) throws IOException {
		try {
			
			StringBuilder sb = new StringBuilder();
			String prefix = "";
			for (int i = 0; i < input.size(); i++) {
				final Object object = input.get(i);
				if (object instanceof Tuple) {
					for (int j = 0; j < ((Tuple) object).size(); j++) {
						sb.append(prefix);
						sb.append(flag);
						sb.append((String) ((Tuple) object).get(j));
						prefix = " || ";
					}
				}

			}
			return sb.toString();
		} catch (ExecException exp) {
			throw exp;
		} catch (Exception e) {
			int errCode = 2106;
			String msg = "Error while computing concat in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}

}
