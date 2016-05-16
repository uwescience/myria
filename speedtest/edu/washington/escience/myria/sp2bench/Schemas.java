package edu.washington.escience.myria.sp2bench;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

public class Schemas {

  final static ImmutableList<Type> triplesTypes =
      ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> triplesColumnNames =
      ImmutableList.of("subject", "predicate", "object");

  final static Schema triplesSchema = new Schema(triplesTypes, triplesColumnNames);

  final static ImmutableList<Type> dictTypes = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
  final static ImmutableList<String> dictColumnNames = ImmutableList.of("ID", "val");
  final static Schema dictSchema = new Schema(dictTypes, dictColumnNames);

  final static ImmutableList<Type> subjectTypes = ImmutableList.of(Type.LONG_TYPE);
  final static ImmutableList<String> subjectColumnNames = ImmutableList.of("subject");
  final static Schema subjectSchema = new Schema(subjectTypes, subjectColumnNames);

  final static ImmutableList<Type> objectTypes = ImmutableList.of(Type.LONG_TYPE);
  final static ImmutableList<String> objectColumnNames = ImmutableList.of("subject");
  final static Schema objectSchema = new Schema(objectTypes, objectColumnNames);

  final static ImmutableList<Type> subjectObjectTypes =
      ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> subjectObjectColumnNames =
      ImmutableList.of("subject", "object");
  final static Schema subjectObjectSchema =
      new Schema(subjectObjectTypes, subjectObjectColumnNames);

  final static ImmutableList<Type> valTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> valColumnNames = ImmutableList.of("val");
  final static Schema valSchema = new Schema(valTypes, valColumnNames);
}
