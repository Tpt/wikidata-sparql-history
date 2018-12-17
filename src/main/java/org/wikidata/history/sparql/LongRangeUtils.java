package org.wikidata.history.sparql;

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

final class LongRangeUtils {

  static boolean isInRange(long element, long[] range) {
    for (int i = 0; i < range.length; i += 2) {
      if (range[i] <= element && element < range[i + 1]) {
        return true;
      }
    }
    return false;
  }

  static boolean isRangeStart(long element, long[] range) {
    for (int i = 0; i < range.length; i += 2) {
      if (range[i] == element) {
        return true;
      }
    }
    return false;
  }

  static boolean isRangeEnd(long element, long[] range) {
    for (int i = 1; i < range.length; i += 2) {
      if (range[i] == element) {
        return true;
      }
    }
    return false;
  }

  static long[] intersection(long[] a, long[] b) {
    if (a.length == 0 || b.length == 0) {
      return null;
    } else if (a.length == 2 && b.length == 2) {
      //Simple case optimization
      long start = Math.max(a[0], b[0]);
      long end = Math.min(a[1], b[1]);
      return (end > start) ? new long[]{start, end} : null;
    } else {
      LongArrayList result = new LongArrayList();
      for (int i = 0; i < a.length; i += 2) {
        for (int j = 0; j < b.length; j += 2) {
          long start = Math.max(a[i], b[j]);
          long end = Math.min(a[i + 1], b[j + 1]);
          if (end > start) {
            result.add(start);
            result.add(end);
          }
        }
      }
      return result.toArray();
    }
  }

  static long[] union(long[] a, long[] b) {
    if (a.length == 0) {
      return b;
    } else if (b.length == 0) {
      return a;
    } else if (a.length == 2 && b.length == 2) {
      //Simple case optimization
      if (a[1] < b[0]) {
        return new long[]{a[0], a[1], b[0], b[1]};
      } else if (b[1] < a[0]) {
        return new long[]{b[0], b[1], a[0], a[1]};
      } else {
        return new long[]{
                Math.min(a[0], b[0]),
                Math.max(a[1], b[1])
        };
      }
    } else {
      LongArrayList result = new LongArrayList();
      for (int i = 0, j = 0; i < a.length || j < b.length; ) {
        if (i < a.length && (j >= b.length || a[i] <= b[j])) {
          if (!result.isEmpty() && result.getLast() >= a[i]) {
            result.set(result.size() - 1, Math.max(result.getLast(), a[i + 1]));
          } else {
            result.add(a[i]);
            result.add(a[i + 1]);
          }
          i += 2;
        } else {
          if (!result.isEmpty() && result.getLast() >= b[j]) {
            result.set(result.size() - 1, Math.max(result.getLast(), b[j + 1]));
          } else {
            result.add(b[j]);
            result.add(b[j + 1]);
          }
          j += 2;
        }
      }
      return result.toArray();
    }
  }

  static boolean isSorted(long[] array) {
    for (int i = 1; i < array.length; i++) {
      if (array[i] <= array[i - 1]) {
        return false;
      }
    }
    return true;
  }
}
