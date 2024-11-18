package com.sqlrec;

public final class Baz implements org.apache.calcite.runtime.ArrayBindable {
    public static class Record1_0 implements java.io.Serializable {
        public long f0;

        public Record1_0() {
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Record1_0)) {
                return false;
            }
            return this.f0 == ((Record1_0) o).f0;
        }

        public int hashCode() {
            int h = 0;
            h = org.apache.calcite.runtime.Utilities.hash(h, this.f0);
            return h;
        }

        public int compareTo(Record1_0 that) {
            final int c;
            c = org.apache.calcite.runtime.Utilities.compare(this.f0, that.f0);
            if (c != 0) {
                return c;
            }
            return 0;
        }

        public String toString() {
            return "{f0=" + this.f0 + "}";
        }

    }

    public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
        final org.apache.calcite.linq4j.Enumerable _inputEnumerable = org.apache.calcite.schema.Schemas.enumerable((org.apache.calcite.schema.ScannableTable) root.getRootSchema().getSubSchema("mySchema").getTable("myTable"), root);
        final org.apache.calcite.linq4j.AbstractEnumerable child = new org.apache.calcite.linq4j.AbstractEnumerable() {
            public org.apache.calcite.linq4j.Enumerator enumerator() {
                return new org.apache.calcite.linq4j.Enumerator() {
                    public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();

                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        while (inputEnumerator.moveNext()) {
                            if (org.apache.calcite.runtime.SqlFunctions.toInt(((Object[]) inputEnumerator.current())[0]) > 1) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        final Object[] current = (Object[]) inputEnumerator.current();
                        final Object input_value = current[0];
                        final Object input_value0 = current[1];
                        return new Object[]{
                                input_value,
                                input_value0};
                    }

                };
            }

        };
        java.util.List accumulatorAdders = new java.util.LinkedList();
        accumulatorAdders.add(new org.apache.calcite.linq4j.function.Function2() {
                                  public Record1_0 apply(Record1_0 acc, Object[] in) {
                                      acc.f0++;
                                      return acc;
                                  }

                                  public Record1_0 apply(Object acc, Object in) {
                                      return apply(
                                              (Record1_0) acc,
                                              (Object[]) in);
                                  }
                              }
        );
        org.apache.calcite.adapter.enumerable.AggregateLambdaFactory lambdaFactory = new org.apache.calcite.adapter.enumerable.BasicAggregateLambdaFactory(
                new org.apache.calcite.linq4j.function.Function0() {
                    public Object apply() {
                        long a0s0;
                        a0s0 = 0L;
                        Record1_0 record0;
                        record0 = new Record1_0();
                        record0.f0 = a0s0;
                        return record0;
                    }
                }
                ,
                accumulatorAdders);
        return child.groupBy(new org.apache.calcite.linq4j.function.Function1() {
                                 public String apply(Object[] a0) {
                                     return a0[1] == null ? null : a0[1].toString();
                                 }

                                 public Object apply(Object a0) {
                                     return apply(
                                             (Object[]) a0);
                                 }
                             }
                , lambdaFactory.accumulatorInitializer(), lambdaFactory.accumulatorAdder(), lambdaFactory.resultSelector(new org.apache.calcite.linq4j.function.Function2() {
                                                                                                                             public Object[] apply(String key, Record1_0 acc) {
                                                                                                                                 return new Object[]{
                                                                                                                                         key,
                                                                                                                                         acc.f0};
                                                                                                                             }

                                                                                                                             public Object[] apply(Object key, Object acc) {
                                                                                                                                 return apply(
                                                                                                                                         (String) key,
                                                                                                                                         (Record1_0) acc);
                                                                                                                             }
                                                                                                                         }
                ));
    }


    public Class getElementType() {
        return java.lang.Object[].class;
    }


}