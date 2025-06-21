package MicroChallenger;

import org.apache.flink.api.common.functions.MapFunction;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.IIORegistry;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

public class BatchDeserializer implements MapFunction<byte[], Batch> {
    @Override
    public Batch map(byte[] batchData) throws Exception {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(batchData)) {
            MapValue batchMap = unpacker.unpackValue().asMapValue();

            // Converti le chiavi stringa in Value prima di cercarle
            Value batchId = batchMap.map().get(ValueFactory.newString("batch_id"));
            Value printId = batchMap.map().get(ValueFactory.newString("print_id"));
            Value tileId = batchMap.map().get(ValueFactory.newString("tile_id"));
            Value layer = batchMap.map().get(ValueFactory.newString("layer"));
            byte[] tiff = batchMap.map().get(ValueFactory.newString("tif")).asBinaryValue().asByteArray();
            int[][] temperatures = parseTiffToTemperatures(tiff);

            return new Batch(
                    batchId.asIntegerValue().toInt(),
                    printId.asStringValue().toString(),
                    tileId.asIntegerValue().toInt(),
                    layer.asIntegerValue().toInt(),
                    temperatures
            );
        }
    }
    private static int[][] parseTiffToTemperatures(byte[] tiffBytes) throws IOException {
        IIORegistry.getDefaultInstance().registerServiceProvider(
                com.twelvemonkeys.imageio.plugins.tiff.TIFFImageReaderSpi.class
        );

        ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes);
        ImageInputStream iis = ImageIO.createImageInputStream(bais);

        Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
        if (!readers.hasNext()) {
            throw new IOException("No ImageReader found for input TIFF data.");
        }

        ImageReader reader = readers.next();
        reader.setInput(iis);

        BufferedImage image = reader.read(0);
        int width = image.getWidth();
        int height = image.getHeight();
        WritableRaster raster = image.getRaster(); // o casta a WritableRaster se necessario
        DataBuffer dataBuffer = raster.getDataBuffer();
        DataBufferUShort ushortBuffer = (DataBufferUShort) dataBuffer;
        short[] rawData = ushortBuffer.getData();



        int[][] temperatures = new int[height][width];

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int index = y * width + x;
                temperatures[y][x] = rawData[index] & 0xFFFF;
            }
        }

        return temperatures;
    }
}