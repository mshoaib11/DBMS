package dbs_project.query.impl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Operator;
import dbs_project.storage.Type;
import dbs_project.storage.impl.ColumnImpl;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class ComparisonStringBuilder
{

	// Constants for the generated code
	static final String EQUALS = "equals";
	static final String ROW_ID = "rowId";
	static final String COMPARE_TO = "compareTo";
	private final Map<String, ColumnImpl> colMap;

	// Operandslist
	private final List<Constant> operandList = new ArrayList<>();
	// Operator for comparisons
	private Operator operatorCompare;

	// the type of the current comparison
	private Type type;

	public ComparisonStringBuilder(Map<String, ColumnImpl> colMap)
	{

		this.colMap = colMap;
	}

	private boolean isOperatorNull()
	{

		return operatorCompare == null;
	}

	private int getOperandsSize()
	{
		if (operandList != null)
		{
			return operandList.size();
		}
		return 0;
	}

	private boolean isTypeNull()
	{
		return type == null;
	}

	//Create a String with operands something like ((A,B))((D,D)((C,D))))))))
	public String build()
	{
		String result = "";

		if (!(isOperatorNull()))
		{
			if (operandList.size() > 1)
			{

				for (int i = 1; i < getOperandsSize(); i++)
				{
					if (result.length() > 0)
					{
						result += " " + Operator.AND + " ";
					}

					String leftHs = getStringFromConstant(operandList.get(i - 1));
					String rightHs = getStringFromConstant(operandList.get(i));

					if (getType() == Type.STRING)
					{
						result += leftHs + ".";

						if (getOperator() == Operator.EQ)
						{
							result += EQUALS + "(" + rightHs + ")";
						}
						else
						{
							result += COMPARE_TO + "(" + rightHs + ") "
									+ getOperator() + " 0";
						}
					}
					else
					{
						result += leftHs + " " + getOperator() + " " + rightHs;
					}
				}
			}
			else
			{
				throw new IllegalStateException("Operation " + operatorCompare
						+ " needs two or more operands. Found: " + operandList);
			}
		}
		return result;
	}

	public void clear()
	{
		operatorCompare = null;
		type = null;
		operandList.clear();
	}

	public void addConstant(Constant c)
	{

		operandList.add(c);
	}

	public Operator getOperator()
	{

		return operatorCompare;
	}

	public void setOperator(Operator oprt)
	{
		if (!isOperatorNull() && operatorCompare != oprt)
		{
			throw new IllegalStateException(
					"Different operators from the same comparison expression: "
							+ operatorCompare + " and " + oprt);
		}
		else
		{
			operatorCompare = oprt;
		}
	}

	public Type getType()
	{

		return type;
	}

	public void setType(Type tp)
	{
		if (!isTypeNull() && type != tp)
		{
			throw new IllegalStateException(
					"Different types on same expression: " + type + " and "
							+ tp);
		}
		else
		{
			type = tp;
		}
	}

	public Type getColumnType(ColumnImpl column)
	{
		return column.getMetaData().getType();
	}

	private String getStringFromConstant(Constant c)
	{
		switch (c.getType())
		{
			case COLUMN_NAME:
				ColumnImpl column = colMap.get(c.getValue());
				StringBuilder string = new StringBuilder(c.getValue());
				switch (getColumnType(column))
				{
					case BOOLEAN:
						string.append(".getBoolean(" + ROW_ID);
						break;
					case DATE:
						string.append(".getDate(" + ROW_ID);
						break;
					case DOUBLE:
						string.append(".getDouble(" + ROW_ID);
						break;
					case INTEGER:
						string.append(".getInteger(" + ROW_ID);
						break;
					case STRING:
						string.append(".getString(" + ROW_ID);
						break;
					default:
						break;
				}
				string.append(")");
				return string.toString();
			case VALUE_LITERAL:
				return getType() == Type.STRING ? "\"" + c.getValue() + "\""
						: c.getValue();// quotes
			default:
				throw new IllegalStateException("Unsupported type: "
						+ c.getType());
		}
	}
}
