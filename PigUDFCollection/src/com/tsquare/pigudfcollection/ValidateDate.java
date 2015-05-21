package com.tsquare.pigudfcollection;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ValidateDate extends EvalFunc<String> {

	public String exec(Tuple input) throws IOException, NullPointerException {

		int tupleSize = input.size();
		if (input == null || tupleSize != 2) {
			return null;
		}

		String inputDate = (String) input.get(0);
		String inputFormat = (String) input.get(1);
		SimpleDateFormat inputDateformat = new SimpleDateFormat(inputFormat);
		SimpleDateFormat impalaDateformat = new SimpleDateFormat(
				"YYYY-MM-DD hh:mm:ss");

		try {

			if (inputDate == "" || input.size() == 0 || inputDate == null) {
				return null;
			} else {
				Date impaladate = impalaDateformat.parse("1900-01-01 00:00:00");
				Date date1 = inputDateformat.parse(inputDate);
				if (date1.compareTo(impaladate) < 0) {
					return impaladate.toString();
				} else {
					return inputDate.toString();
				}
			}
		} catch (ParseException e) {
			return "1900-01-01 00:00:00";
		}

	}
}
