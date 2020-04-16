package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;

class FragmentationUtils {
  static final int FRAME_OFFSET = // 9 bytes in total
      FrameLengthFlyweight.FRAME_LENGTH_SIZE // includes encoded frame length bytes size
          + FrameHeaderFlyweight.size(); // includes encoded frame headers info bytes size
  static final int FRAME_OFFSET_WITH_METADATA = // 12 bytes in total
      FRAME_OFFSET
          + FrameLengthFlyweight.FRAME_LENGTH_SIZE; // include encoded metadata length bytes size

  static final int FRAME_OFFSET_WITH_INITIAL_REQUEST_N = // 13 bytes in total
      FRAME_OFFSET + Integer.BYTES; // includes extra space for initialRequestN bytes size
  static final int FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N = // 16 bytes in total
      FRAME_OFFSET_WITH_METADATA
          + Integer.BYTES; // includes extra space for initialRequestN bytes size

  static boolean isFragmentable(
      int mtu, ByteBuf data, ByteBuf metadata, boolean hasMetadata, boolean hasInitialRequestN) {
    if (mtu == 0) {
      return false;
    }

    if (hasMetadata) {
      int remaining =
          mtu
              - (hasInitialRequestN
                  ? FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N
                  : FRAME_OFFSET_WITH_METADATA);

      return (metadata.readableBytes() + data.readableBytes()) > remaining;
    } else {
      int remaining =
          mtu - (hasInitialRequestN ? FRAME_OFFSET_WITH_INITIAL_REQUEST_N : FRAME_OFFSET);

      return data.readableBytes() > remaining;
    }
  }

  static ByteBuf encodeFollowsFragment(
      ByteBufAllocator allocator,
      int mtu,
      int streamId,
      boolean complete,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length size
    int remaining = mtu - FRAME_OFFSET;

    ByteBuf metadataFragment = null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthFlyweight.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    if (remaining > 0 && data.isReadable()) {
      int r = Math.min(remaining, data.readableBytes());
      dataFragment = data.readRetainedSlice(r);
    }

    boolean follows = data.isReadable() || metadata.isReadable();
    return PayloadFrameFlyweight.encode(
        allocator, streamId, follows, (!follows && complete), true, metadataFragment, dataFragment);
  }

  static ByteBuf encodeFirstFragment(
      ByteBufAllocator allocator,
      int mtu,
      FrameType frameType,
      int streamId,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length size
    int remaining = mtu - FRAME_OFFSET;

    ByteBuf metadataFragment = null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthFlyweight.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    if (remaining > 0 && data.isReadable()) {
      int r = Math.min(remaining, data.readableBytes());
      dataFragment = data.readRetainedSlice(r);
    }

    switch (frameType) {
      case REQUEST_FNF:
        return RequestFireAndForgetFrameFlyweight.encode(
            allocator, streamId, true, metadataFragment, dataFragment);
      case REQUEST_RESPONSE:
        return RequestResponseFrameFlyweight.encode(
            allocator, streamId, true, metadataFragment, dataFragment);
        // Payload and synthetic types from the responder side
      case PAYLOAD:
        return PayloadFrameFlyweight.encode(
            allocator, streamId, true, false, false, metadataFragment, dataFragment);
      case NEXT:
        // see https://github.com/rsocket/rsocket/blob/master/Protocol.md#handling-the-unexpected
        // point 7
      case NEXT_COMPLETE:
        return PayloadFrameFlyweight.encode(
            allocator, streamId, true, false, true, metadataFragment, dataFragment);
      default:
        throw new IllegalStateException("unsupported fragment type: " + frameType);
    }
  }

  static ByteBuf encodeFirstFragment(
      ByteBufAllocator allocator,
      int mtu,
      int initialRequestN,
      FrameType frameType,
      int streamId,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length bytes + initial requestN bytes
    int remaining = mtu - FRAME_OFFSET_WITH_INITIAL_REQUEST_N;

    ByteBuf metadataFragment = null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthFlyweight.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    if (remaining > 0 && data.isReadable()) {
      int r = Math.min(remaining, data.readableBytes());
      dataFragment = data.readRetainedSlice(r);
    }

    switch (frameType) {
        // Requester Side
      case REQUEST_STREAM:
        return RequestStreamFrameFlyweight.encode(
            allocator, streamId, true, initialRequestN, metadataFragment, dataFragment);
      case REQUEST_CHANNEL:
        return RequestChannelFrameFlyweight.encode(
            allocator, streamId, true, false, initialRequestN, metadataFragment, dataFragment);
      default:
        throw new IllegalStateException("unsupported fragment type: " + frameType);
    }
  }
}
