use lz4_flex::{compress_prepend_size, decompress_size_prepended};

#[derive(Debug, Clone)]
pub struct Lz4CompressionController {}

impl Lz4CompressionController {
    pub(crate) fn compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let compressed_data = compress_prepend_size(ori);

        Ok(compressed_data)
    }

    pub(crate) fn de_compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let decompressed_data = decompress_size_prepended(&ori)
            .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?;
        Ok(decompressed_data)
    }
}
