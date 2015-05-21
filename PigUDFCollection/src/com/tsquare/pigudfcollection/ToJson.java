package com.tsquare.pigudfcollection;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class ToJson extends EvalFunc<String> {
	public static int j = 1;

	public String exec(Tuple value) throws IOException, NullPointerException {

		try {
			String[] field_type = value.get(1).toString().split(",");
			String input = (String) value.get(0);
			if (input != null) {
				String[] value_split = input.replaceAll("\\s\\|\\|\\s", "||")
						.split("~");
				String app = "[";

				for (String s : value_split) {
					String[] each_value = s.split("\\|\\|");

					app += "{";
					for (int i = 0; i < each_value.length; i++) {
						String[] flag = each_value[i].split(":");
						for (; j <= field_type.length; j++) {
							String[] each_field = field_type[j - 1].split("_");

							if (Integer.parseInt(flag[1]) == j) {
								app += "\"" + each_field[0] + "\"" + ":";
								if (each_field[1].equals("string"))
									app += "\"" + flag[0] + "\"" + ",";

								if (each_field[1].equals("array")) {
									String arr[] = flag[0].split("[+]");
									app += "[";
									for (String str : arr)
										app += "\"" + str + "\"" + ",";
									app = app.substring(0, app.length() - 1)
											+ "]" + ",";
								}
								j = Integer.parseInt(flag[1]) + 1;
								if (i != each_value.length - 1)
									break;
								j--;
							} else {
								if (each_field[1].equals("string"))
									app += "\"" + each_field[0] + "\"" + ":"
											+ "\"\"" + ",";
								if (each_field[1].equals("array"))
									app += "\"" + each_field[0] + "\"" + ":"
											+ "[" + "]" + ",";
							}

						}

					}
					app = app.substring(0, app.length() - 1) + "}" + ",";
					j = 1;
				}
				app = app.substring(0, app.length() - 1);
				return app += "]";
			}
			return null;
		}

		catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

}
