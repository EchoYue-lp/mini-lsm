use snap::raw::{Decoder, Encoder, decompress_len, max_compress_len};

#[derive(Debug, Clone)]
pub struct SnappyCompressionController {}

impl SnappyCompressionController {
    pub(crate) fn compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let max_compressed_len = max_compress_len(ori.len());
        let mut compressed_buffer = vec![0; max_compressed_len];

        let compressed_len = Encoder::new()
            .compress(ori, &mut compressed_buffer)
            .map_err(|e| anyhow::anyhow!("Snappy compression failed: {}", e))?;

        compressed_buffer.truncate(compressed_len);
        Ok(compressed_buffer)
    }

    pub(crate) fn de_compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let decompressed_len = decompress_len(ori)
            .map_err(|e| anyhow::anyhow!("Snappy decompression length check failed: {}", e))?;

        let mut decompressed_buffer = vec![0; decompressed_len];

        Decoder::new()
            .decompress(ori, &mut decompressed_buffer)
            .map_err(|e| anyhow::anyhow!("Snappy decompression failed: {}", e))?;

        Ok(decompressed_buffer)
    }
}
