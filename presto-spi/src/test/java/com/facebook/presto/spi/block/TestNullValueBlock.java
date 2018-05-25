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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestNullValueBlock
{
    @Test
    public void testBuildingFromLongArrayBlockBuilder()
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        //Ensures that the created block is NullValueBlock
        assertEquals(blockBuilder.build().getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyPositions(new int[] {2, 4, 6, 8}, 0, 4).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyRegion(90, 10).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.getRegion(80, 10).getEncodingName(), NullValueBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromIntArrayBlockBuilder()
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyPositions(new int[] {2, 4, 6, 8}, 0, 4).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyRegion(90, 10).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.getRegion(80, 10).getEncodingName(), NullValueBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromShortArrayBlockBuilder()
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyPositions(new int[] {2, 4, 6, 8}, 0, 4).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyRegion(90, 10).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.getRegion(80, 10).getEncodingName(), NullValueBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromByteArrayBlockBuilder()
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyPositions(new int[] {2, 4, 6, 8}, 0, 4).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.copyRegion(90, 10).getEncodingName(), NullValueBlockEncoding.NAME);
        assertEquals(blockBuilder.getRegion(80, 10).getEncodingName(), NullValueBlockEncoding.NAME);
    }

    private void populateNullValues(BlockBuilder blockBuilder, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
    }
}
