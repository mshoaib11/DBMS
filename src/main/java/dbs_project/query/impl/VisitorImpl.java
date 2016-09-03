package dbs_project.query.impl;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import dbs_project.storage.impl.ColumnImpl;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.ExpressionVisitor;
import dbs_project.query.predicate.Operator;
import dbs_project.storage.Type;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class VisitorImpl implements ExpressionVisitor
{


	// Atomic Generator for creating unique classnames
	private static final AtomicLong CLASS_SEQUENCE_GEN;

	private static final ClassPool POOL;

	// Constants for the generated code
	static final String ROW_ID = "rowId";
	static final String NAME_SCHEMA = "colMap";
	static final String NAME_EVAL = "eval";
	static final String INDENT = "    ";

	private final Map<String, ColumnImpl> columnMap;

	// Collects the code for the eval function during visitation.
	private final StringBuilder evalCodeStringBuilder;

	// Keeps state when processing comparison expressions
	private final ComparisonStringBuilder comparisonStringBuilder;

	static
	{
		CLASS_SEQUENCE_GEN = new AtomicLong(0);
		POOL = ClassPool.getDefault();
		POOL.importPackage(DynamicPredicate.class.getPackage().getName());
		POOL.importPackage(ColumnImpl.class.getPackage().getName());
		POOL.importPackage(Map.class.getPackage().getName());
	}

	public VisitorImpl(Map<String, ColumnImpl> columnMap)
	{
		this.columnMap = columnMap;
		this.evalCodeStringBuilder = new StringBuilder();
		this.comparisonStringBuilder = new ComparisonStringBuilder(
				this.columnMap);
	}

	@Override
	public void visitExpression(Expression expression)
	{
		beginExpression();
		processSubExpressions(expression);
		endExpression();
	}

	@Override
	public void visitConstant(Constant constant)
	{
		switch (constant.getType())
		{
			case COLUMN_NAME:
				ColumnImpl column = columnMap.get(constant.getValue());
				comparisonStringBuilder.setType(getColumnType(column));
				break;
			case VALUE_LITERAL:
				break;
			case NULL_LITERAL:
			default:
				throw new IllegalArgumentException(
						"Unsupported constant type: " + constant.getType());
		}

		comparisonStringBuilder.addConstant(constant);
	}

	private void beginExpression()
	{
		evalCodeStringBuilder.append('(');
	}

	private void processSubExpressions(Expression expression)
	{
		final Operator operator = expression.getOperator();

		for (int i = 0; i < expression.getOperandCount(); i++)
		{
			ExpressionElement node = expression.getOperand(i);
			if (i > 0)
			{
				// from the second node...
				if (isBooleanOperator(operator))
				{
					// simply append to string representation
					evalCodeStringBuilder.append(' ').append(operator)
							.append(' ');
				}
				else
				{
					// is comparison operator
					comparisonStringBuilder.setOperator(operator);
				}
			}

			node.accept(this);
		}
	}

	private void endExpression()
	{
		evalCodeStringBuilder.append(comparisonStringBuilder.build());
		evalCodeStringBuilder.append(')');
		// clear state
		comparisonStringBuilder.clear();
	}

	private static boolean isBooleanOperator(Operator op)
	{
		return op == Operator.AND || op == Operator.OR;
	}

	public DynamicPredicate createPredicate() throws Exception
	{
		String className = "DynamicPredicate_"
				+ CLASS_SEQUENCE_GEN.getAndIncrement();

		// make a new ct class
		CtClass ctDynClass = POOL.makeClass(className);

		// make it implement the DynamicPredicate interface
		ctDynClass.addInterface(POOL.get(DynamicPredicate.class
				.getCanonicalName()));

		// create all necessary fields from the schema
		generateFields(ctDynClass);

		// create a constructor that takes the schema
		generateContructor(ctDynClass);

		// create eval(...) method from the result of the visitor's AST
		// traversal
		generateEvalMethod(ctDynClass);

		// Instantiate a new classloader for the dynamic class that has the
		// context classloader as parent. Necessary to get rid of the generated
		// class later and to avoid running out of memory
		ClassLoader classLoader = new ClassLoader(Thread.currentThread()
				.getContextClassLoader())
		{
		};

		// transform the CtClass into a Class and load it
		Class<?> _class = ctDynClass.toClass(classLoader,
				ColumnImpl.class.getProtectionDomain());

		// create instance by calling the created contructor
		Constructor<?> constructor = _class.getConstructor(Map.class);

		return (DynamicPredicate) constructor.newInstance(columnMap);
	}

	private void generateFields(CtClass ctClass) throws CannotCompileException
	{


		for (ColumnImpl cd : columnMap.values())
		{
			String fieldString = columnDescriptorToFieldString(cd);

			ctClass.addField(CtField.make(fieldString, ctClass));
		}
	}

	private void generateContructor(CtClass ctClass)
			throws CannotCompileException
	{
		String constructorCode = "public " + ctClass.getSimpleName() + "("
				+ Map.class.getSimpleName() + " " + NAME_SCHEMA + ") {\n";

		for (ColumnImpl cd : columnMap.values())
		{
			constructorCode += columnDescriptorToInitString(cd);
		}
		constructorCode += "}";



		ctClass.addConstructor(CtNewConstructor.make(constructorCode, ctClass));
	}

	private void generateEvalMethod(CtClass ctClass)
			throws CannotCompileException
	{
		String evalMethod = "public boolean " + NAME_EVAL + "(int " + ROW_ID
				+ ") {\n" + INDENT + "return "
				+ evalCodeStringBuilder.toString() + ";\n" + "}";


		ctClass.addMethod(CtNewMethod.make(evalMethod, ctClass));
	}

	private static String columnDescriptorToFieldString(ColumnImpl column)
	{
		StringBuilder field = new StringBuilder("private final ");

		field.append(ColumnImpl.class.getSimpleName());
		field.append(" ");
		field.append(column.getMetaData().getName());
		field.append(";\n");

		return field.toString();
	}

	private static String columnDescriptorToInitString(ColumnImpl column)
	{
		String initCode = INDENT + "this." + column.getMetaData().getName()
				+ " = (" + ColumnImpl.class.getSimpleName() + ")" + NAME_SCHEMA
				+ ".get(\"" + column.getMetaData().getName() + "\")" + ";\n";

		return initCode;
	}

	public Type getColumnType(ColumnImpl column)
	{
		return column.getMetaData().getType();
	}
}
