package com.tsquare.pigudfcollection;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StringConcatWithDelim extends EvalFunc<String> {

	private final String delim;

	public StringConcatWithDelim(String delim) {
		this.delim = String.valueOf(delim);
	}

	@Override
	public String exec(Tuple input) throws IOException {
		try {
			if (input == null || input.size() == 0)
				return null;

			StringBuilder sb = new StringBuilder();
			String prefix = "";
			for (int i = 0; i < input.size(); i++) {
				if (input.get(i) != null && !input.get(i).toString().isEmpty()) {
					sb.append(prefix);
					sb.append(String.valueOf(input.get(i)));
					prefix = delim;
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
