/*
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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;

public class NullValueBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NullValueBlock.class).instanceSize();

    private final int positionCount;
    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public NullValueBlock(int positionCount)
    {
        this.positionCount = positionCount;
        this.sizeInBytes = Integer.BYTES;
        this.retainedSizeInBytes = Integer.BYTES + INSTANCE_SIZE;
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.appendNull();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new NullValueBlock(1);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return NullValueBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        return new NullValueBlock(length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new NullValueBlock(length);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(getPositionCount(), position, length);

        return new NullValueBlock(length);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return true;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    //For hashing purpose and for accessing null values
    @Override
    public long getLong(int position, int offset)
    {
        return 0;
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return 0;
    }

    @Override
    public short getShort(int position, int offset)
    {
        return 0;
    }

    @Override
    public int getInt(int position, int offset)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return Slices.EMPTY_SLICE;
    }
}
