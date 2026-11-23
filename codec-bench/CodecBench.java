import com.github.luben.zstd.Zstd;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Intrinsic characteristics of the codecs used by Spark/Parquet, measured
 * with the very same libraries Spark ships (lz4-java, zstd-jni). Single thread, data split into independent blocks like a
 * shuffle stream (32 KB) or a Parquet page (1 MB).
 *
 * Usage: java -cp ".:$SPARK_HOME/jars/*" CodecBench <file> <blockKB> <reps> [header]
 * Output: one CSV line per codec and repetition.
 */
public class CodecBench {

    interface Codec {
        String name();
        byte[] compress(byte[] in) throws Exception;
        byte[] decompress(byte[] in, int originalLength) throws Exception;
    }

    static Codec lz4() {
        final LZ4Factory f = LZ4Factory.fastestInstance();
        final LZ4Compressor c = f.fastCompressor();
        final LZ4FastDecompressor d = f.fastDecompressor();
        return new Codec() {
            public String name() { return "lz4"; }
            public byte[] compress(byte[] in) { return c.compress(in); }
            public byte[] decompress(byte[] in, int n) { return d.decompress(in, n); }
        };
    }

    static Codec zstd(final int level) {
        return new Codec() {
            public String name() { return "zstd-" + level; }
            public byte[] compress(byte[] in) { return Zstd.compress(in, level); }
            public byte[] decompress(byte[] in, int n) { return Zstd.decompress(in, n); }
        };
    }

    public static void main(String[] args) throws Exception {
        String file = args[0];
        int blockKB = Integer.parseInt(args[1]);
        int reps = Integer.parseInt(args[2]);
        boolean header = args.length > 3 && args[3].equals("header");

        byte[] data = Files.readAllBytes(Paths.get(file));
        int bs = blockKB * 1024;
        List<byte[]> blocks = new ArrayList<>();
        for (int off = 0; off < data.length; off += bs) {
            blocks.add(Arrays.copyOfRange(data, off, Math.min(data.length, off + bs)));
        }
        String input = Paths.get(file).getFileName().toString();
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();

        Codec[] codecs = {lz4(), zstd(1), zstd(3), zstd(9)};
        if (header) {
            System.out.println("input,block_kb,codec,rep,original_bytes,compressed_bytes,ratio,"
                    + "compress_s,compress_cpu_s,decompress_s,decompress_cpu_s,compress_MBps,decompress_MBps");
        }
        for (Codec codec : codecs) {
            for (int rep = 0; rep <= reps; rep++) {          // rep 0 = JIT warm-up, not printed
                byte[][] comp = new byte[blocks.size()][];
                long c0 = bean.getCurrentThreadCpuTime(), w0 = System.nanoTime();
                for (int i = 0; i < blocks.size(); i++) comp[i] = codec.compress(blocks.get(i));
                long w1 = System.nanoTime(), c1 = bean.getCurrentThreadCpuTime();
                long restored = 0;
                for (int i = 0; i < blocks.size(); i++) {
                    restored += codec.decompress(comp[i], blocks.get(i).length).length;
                }
                long w2 = System.nanoTime(), c2 = bean.getCurrentThreadCpuTime();
                if (restored != data.length) throw new IllegalStateException(codec.name() + ": size mismatch");
                long compressed = 0;
                for (byte[] c : comp) compressed += c.length;
                if (rep == 0) continue;
                double cs = (w1 - w0) / 1e9, ds = (w2 - w1) / 1e9;
                System.out.printf("%s,%d,%s,%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.1f,%.1f%n",
                        input, blockKB, codec.name(), rep, (long) data.length, compressed,
                        (double) data.length / compressed, cs, (c1 - c0) / 1e9, ds, (c2 - c1) / 1e9,
                        data.length / 1e6 / cs, data.length / 1e6 / ds);
            }
        }
    }
}
