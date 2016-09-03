/*
 * Copyright(c) 2012 Saarland University - Information Systems Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbs_project.util;

import dbs_project.storage.Type;
import dbs_project.util.zipf.ZipfDistributionFromGrayEtAl;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Helper class for generating TPC-H like datasets
 */
public class TPCHData {

    public static int CUSTOMER_BASE_SIZE = 150;

    public static int ORDERS_BASE_SIZE = 1500;

    public static int LINEITEM_BASE_SIZE = 6000;

    public static int PART_BASE_SIZE = 200;

    public static int SUPP_BASE_SIZE = 10;

    public static final double THETA = 0.5;

    public static final int WORD_COUNT = 10000;

    public static final char[] LITERALS = {'0','1','2','3','4','5','6','7','8','9'};

    public static final String[] MKTSEGMENT = {
        "AUTOMOBILE", "BUILDING" ,"FURNITURE", "MACHINERY", "HOUSEHOLD"
    };

    public static final String[] ORDERSTATUS = {"F", "O", "P"};

    public static final String[] ORDERPRIORTIY = {
        "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"
    };

    public static final String[] INSTRUCT = {
        "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"
    };

    public static final String[] MODE = {
        "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"
    };

    public static final String[] RETURNFLAG = { "N", "R", "A" };

    public static final String[] LINESTATUS = { "O", "F" };


    public static int getBaseSize(String relation) {
        switch (relation.toLowerCase()) {
            case "customer":
                return CUSTOMER_BASE_SIZE;
            case "orders":
                return ORDERS_BASE_SIZE;
            case "lineitem":
                return LINEITEM_BASE_SIZE;
        }
        throw new IllegalArgumentException("Unknown relation: " + relation);
    }


    public static List<SimpleColumn> createColumns(String relation, int scale, int partition) {
        switch (relation.toLowerCase()) {
            case "customer":
                return createCustomerColumns(scale, partition);
            case "orders":
                return createOrdersColumns(scale, partition);
            case "lineitem":
                return createLineitemColumns(scale, partition);
        }
        throw new IllegalArgumentException("Unknown relation: " + relation);
    }


    public static List<SimpleColumn> createCustomerColumns(int scale, int partition) {
        int size = CUSTOMER_BASE_SIZE;
        List<SimpleColumn> columns = new ArrayList<>(8);
        String[] words = new String[WORD_COUNT];
        int id = 0;

        for (int i = 0; i < WORD_COUNT; ++i) {
            words[i] = Utils.generateRandomString(Utils.RANDOM.nextInt(10));
        }

        columns.add(createKeyColumn(id++, "c_custkey", size, partition));
        columns.add(createPrefixedKeyString(id++, "c_name", size, partition, "Customer#"));
        columns.add(createRandomStringColumn(id++, "c_address", size, 40));
        columns.add(createForeignKeyColumn(id++, "c_nationkey", size, 25));
        columns.add(createPhoneColum(id++, "c_phone", size));
        columns.add(createDoubleColumn(id++, "c_acctbal", size, -1000, 10000));
        columns.add(createWordsColumn(id++, "c_mktsegment", size, MKTSEGMENT, 1, 1));
        columns.add(createWordsColumn(id++, "c_comment", size, words, 0, 11));

        return columns;
    }


    public static List<SimpleColumn> createOrdersColumns(int scale, int partition) {
        int size = ORDERS_BASE_SIZE;
        List<SimpleColumn> columns = new ArrayList<>(9);
        String[] words = new String[WORD_COUNT];
        int id = 0;

        for (int i = 0; i < WORD_COUNT; ++i) {
            words[i] = Utils.generateRandomString(Utils.RANDOM.nextInt(10));
        }

        columns.add(createKeyColumn(id++, "o_orderkey", size, partition));
        columns.add(createForeignKeyColumn(id++, "o_custkey", size, scale * CUSTOMER_BASE_SIZE));
        columns.add(createWordsColumn(id++, "o_orderstatus", size, ORDERSTATUS, 1, 1));
        columns.add(createDoubleColumn(id++, "o_totalprice", size, 1, 600000));
        columns.add(createDateColumn(id++, "o_orderdate", size, 694224000, 915148800));
        columns.add(createWordsColumn(id++, "o_orderpriority", size, ORDERPRIORTIY, 1, 1));
        columns.add(createPrefixedForeignKeyString(id++, "o_clerk", size, "Clerk#"));
        columns.add(createSingleValueColumn(id++, "o_shippriority", size));
        columns.add(createWordsColumn(id++, "o_comment", size, words, 0, 8));

        return columns;
    }


    public static List<SimpleColumn> createLineitemColumns(int scale, int partition) {
        int size = LINEITEM_BASE_SIZE;
        List<SimpleColumn> columns = new ArrayList<>(16);
        String[] words = new String[WORD_COUNT];
        int id = 0;

        for (int i = 0; i < WORD_COUNT; ++i) {
            words[i] = Utils.generateRandomString(Utils.RANDOM.nextInt(10));
        }

        columns.add(createSortedForeignKeyColumn(id++, "l_orderkey", size, ORDERS_BASE_SIZE, partition));
        columns.add(createForeignKeyColumn(id++, "l_partkey", size, scale * PART_BASE_SIZE));
        columns.add(createForeignKeyColumn(id++, "l_suppkey", size, scale * SUPP_BASE_SIZE));
        columns.add(createIntColumn(id++, "l_linenumber", size, 1, 7));
        columns.add(createIntColumn(id++, "l_quantity", size, 1, 50));
        columns.add(createDoubleColumn(id++, "l_extendedprice", size, 1, 100000));
        columns.add(createLowCardinalityDoubleColumn(id++, "l_discount", size, 0.0, 0.01, 11));
        columns.add(createLowCardinalityDoubleColumn(id++, "l_tax", size, 0.0, 0.01, 9));
        columns.add(createWordsColumn(id++, "l_returnflag", size, RETURNFLAG, 1, 1));
        columns.add(createWordsColumn(id++, "l_linestatus", size, LINESTATUS, 1, 1));
        columns.add(createDateColumn(id++, "l_shipdate", size, 694224000, 915148800));
        columns.add(createDateColumn(id++, "l_commitdate", size, 694224000, 915148800));
        columns.add(createDateColumn(id++, "l_receiptdate", size, 694224000, 915148800));
        columns.add(createWordsColumn(id++, "l_shipinstruct", size, INSTRUCT, 1, 1));
        columns.add(createWordsColumn(id++, "l_shipmode", size, MODE, 1, 1));
        columns.add(createWordsColumn(id++, "l_comment", size, words, 0, 4));

        return columns;
    }


    public static SimpleColumn createKeyColumn(int id, String name, int size, int partition) {
        List<Integer> data = new ArrayList<>(size);

        for (int i = partition * size + 1; i <= (partition + 1) * size; ++i) {
            data.add(i);
        }
        return new SimpleColumn(data, id, name, Type.INTEGER);
    }


    public static SimpleColumn createForeignKeyColumn(int id, String name, int size, int range) {
        ZipfDistributionFromGrayEtAl dist = new ZipfDistributionFromGrayEtAl(range, THETA, Utils.RANDOM.nextInt());
        List<Integer> data = new ArrayList<>(size);

        for (int i = 1; i <= size; ++i) {
            data.add(dist.nextInt());
        }
        return new SimpleColumn(data, id, name, Type.INTEGER);
    }


    public static SimpleColumn createSortedForeignKeyColumn(int id, String name, int size, int range, int partition) {
        List<Integer> data = new ArrayList<>(size);

        int start = partition * size;
        int limit = (partition + 1) * size;
        for (int i = start + 1; i <= limit && data.size() < size; ++i) {
            int count = Utils.RANDOM.nextInt(2 * size / range + 1);

            for (int j = 0; j < count && data.size() < size; ++j) {
                data.add(i);
            }
        }
        while (data.size() < size) { // fill up remaining space
            data.add(limit - 1);
        }
        return new SimpleColumn(data, id, name, Type.INTEGER);
    }


    public static SimpleColumn createWordsColumn(int id, String name, int size, String[] words, int min, int max) {
        ZipfDistributionFromGrayEtAl countDist = new ZipfDistributionFromGrayEtAl(max - min + 1, THETA, Utils.RANDOM.nextInt());
        ZipfDistributionFromGrayEtAl wordDist = new ZipfDistributionFromGrayEtAl(words.length, THETA, Utils.RANDOM.nextInt());
        StringBuilder sb = new StringBuilder();
        List<String> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            sb.delete(0, sb.length());
            int len = min + countDist.nextInt();
            sb.append(words[wordDist.nextInt()]);
            for (int j = 1; j < len; ++j) {
                sb.append(' ');
                sb.append(words[wordDist.nextInt()]);
            }
            data.add(sb.toString());
        }
        return new SimpleColumn(data, id, name, Type.STRING);
    }


    public static SimpleColumn createDateColumn(int id, String name, int size, long min, long max) {
        List<Date> data = new ArrayList<>();

        for (int i = 0; i < size; ++i) {
            long date = min + (nextLong(max - min) / 86400) * 86400;
            data.add(new Date(date));
        }
        return new SimpleColumn(data, id, name, Type.DATE);
    }
    
	public static long nextLong(long n) {
		long bits, val;
		do {
			bits = (Utils.RANDOM.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}


    public static SimpleColumn createSingleValueColumn(int id, String name, int size) {
        List<Integer> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            data.add(0);
        }
        return new SimpleColumn(data, id, name, Type.INTEGER);
    }


    public static SimpleColumn createDoubleColumn(int id, String name, int size, int min, int max) {
        ZipfDistributionFromGrayEtAl dist = new ZipfDistributionFromGrayEtAl(max - min, THETA, Utils.RANDOM.nextInt());
        List<Double> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            double value = min + dist.nextInt();
            value += ((double)Utils.RANDOM.nextInt(100)) / 100.0;
            data.add(value);
        }
        return new SimpleColumn(data, id, name, Type.DOUBLE);
    }


    public static SimpleColumn createIntColumn(int id, String name, int size, int min, int max) {
        ZipfDistributionFromGrayEtAl dist = new ZipfDistributionFromGrayEtAl(max - min + 1, THETA, Utils.RANDOM.nextInt());
        List<Integer> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            data.add(min + dist.nextInt());
        }
        return new SimpleColumn(data, id, name, Type.INTEGER);
    }

    public static SimpleColumn createPhoneColum(int id, String name, int size) {
        StringBuilder sb = new StringBuilder();
        List<String> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            sb.delete(0, sb.length());
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append('-');
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append('-');
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append('-');
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            sb.append(LITERALS[Utils.RANDOM.nextInt(LITERALS.length)]);
            data.add(sb.toString());
        }
        return new SimpleColumn(data, id++, name, Type.STRING);
    }


    public static SimpleColumn createRandomStringColumn(int id, String name, int size, int maxLength) {
        List<String> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            data.add(Utils.generateRandomString(Utils.RANDOM.nextInt(maxLength)));
        }
        return new SimpleColumn(data, id++, name, Type.STRING);
    }


    public static SimpleColumn createLowCardinalityDoubleColumn(int id, String name, int size, double base, double step, int cardinality) {
        List<Double> data = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            data.add(base + Utils.RANDOM.nextInt(cardinality) * step);
        }
        return new SimpleColumn(data, id, name, Type.DOUBLE);
    }


    public static SimpleColumn createPrefixedForeignKeyString(int id, String name, int size, String prefix) {
        List<String> data = new ArrayList<>(size);
        StringBuilder sb = new StringBuilder(prefix);

        for (int i = 1; i <= size; ++i) {
            int number = 1 + Utils.RANDOM.nextInt(1000);
            for (long j = number; j < 100000000l; j *= 10) {
                sb.append('0');
            }
            sb.append(number);
            data.add(sb.toString());
            sb.delete(prefix.length(), sb.length());
        }
        return new SimpleColumn(data, id, name, Type.STRING);
    }


    public static SimpleColumn createPrefixedKeyString(int id, String name, int size, int partition, String prefix) {
        List<String> data = new ArrayList<>(size);
        StringBuilder sb = new StringBuilder(prefix);

        for (int i = partition * size + 1; i <= (partition + 1) * size; ++i) {
            for (long j = i; j < 100000000l; j *= 10) {
                sb.append('0');
            }
            sb.append(i);
            data.add(sb.toString());
            sb.delete(prefix.length(), sb.length());
        }
        return new SimpleColumn(data, id, name, Type.STRING);
    }

}
