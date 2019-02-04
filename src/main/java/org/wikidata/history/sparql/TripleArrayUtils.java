package org.wikidata.history.sparql;


final class TripleArrayUtils {

  static long[] addToSortedArray(long[] array, long[] triple) {
    int position = 0;
    while (position < array.length && (array[position] < triple[0] || (array[position] == triple[0] && (array[position + 1] < triple[1] || (array[position + 1] == triple[1] && array[position + 2] < triple[2]))))) {
      position += 3;
    }

    //Case where it is already in
    if (position < array.length && array[position] == triple[0] && array[position + 1] == triple[1] && array[position + 2] == triple[2]) {
      return array;
    }

    //We build the new array
    long[] newArray = new long[array.length + 3];
    System.arraycopy(array, 0, newArray, 0, position);
    System.arraycopy(triple, 0, newArray, position, 3);
    System.arraycopy(array, position, newArray, position + 3, array.length - position);

    return newArray;
  }

  static long[] removeFromSortedArray(long[] array, long[] triple) {
    int position = 0;
    while (position < array.length && (array[position] != triple[0] || array[position + 1] != triple[1] || array[position + 2] != triple[2])) {
      position += 3;
    }

    //Case where it is not in
    if (position >= array.length) {
      return array;
    }

    //We build the new array
    long[] newArray = new long[array.length - 3];
    System.arraycopy(array, 0, newArray, 0, position);
    System.arraycopy(array, position + 3, newArray, position, array.length - position - 3);
    return newArray;
  }
}
