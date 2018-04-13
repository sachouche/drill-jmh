package org.apache.drill.jmh.common.utils;

/** Exposes functionality to speedup variable length manipulation */
public final class FastAccessUtils {

  public static void vlCopyLELong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    MemoryUtils.putLong(src, srcIndex, dest, destIndex);
  }

  public static void vlCopyGTLong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    final int bulkCopyThreshold = MemoryUtils.LONG_NUM_BYTES * 2;
    if (length < bulkCopyThreshold) {
      _vlCopyGTLong(src, srcIndex, dest, destIndex, length);

    } else {
      System.arraycopy(src, srcIndex, dest, destIndex, length);
    }
  }

  private static void _vlCopyGTLong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    final int numLongEntries = length / MemoryUtils.LONG_NUM_BYTES;
    final int remaining      = length % MemoryUtils.LONG_NUM_BYTES;
    int prevCopied           = 0;

    if (numLongEntries == 1) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      prevCopied = MemoryUtils.LONG_NUM_BYTES;

    } else {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);
      prevCopied = 2 * MemoryUtils.LONG_NUM_BYTES;
    }

    if (remaining > 0) {
      final int srcPos  = srcIndex  + prevCopied;
      final int destPos = destIndex + prevCopied;

      MemoryUtils.putLong(src, srcPos, dest, destPos);
    }
  }

}
