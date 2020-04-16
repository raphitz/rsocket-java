package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_INITIAL_REQUEST_N;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameLengthFlyweight;

final class PayloadValidationUtils {
  static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "The payload is too big to send as a single frame with a 24-bit encoded length. Consider enabling fragmentation via RSocketFactory.";

  static boolean isValid(
      int mtu, ByteBuf data, ByteBuf metadata, boolean hasMetadata, boolean hasInitialRequestN) {
    if (mtu > 0) {
      return true;
    }

    int unitSize;
    if (hasMetadata) {
      unitSize =
          (hasInitialRequestN
                  ? FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N
                  : FRAME_OFFSET_WITH_METADATA)
              + metadata.readableBytes()
              + // metadata payload bytes
              data.readableBytes(); // data payload bytes
    } else {
      unitSize =
          (hasInitialRequestN ? FRAME_OFFSET_WITH_INITIAL_REQUEST_N : FRAME_OFFSET)
              + data.readableBytes(); // data payload bytes
    }

    return unitSize <= FrameLengthFlyweight.FRAME_LENGTH_MASK;
  }
}
