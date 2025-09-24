use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::io::{Read, Write};

#[derive(Debug, Clone)]
pub struct GzCompressionController {}

impl GzCompressionController {
    pub(crate) fn compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut compressed_data = Vec::new();
        let mut encoder = GzEncoder::new(&mut compressed_data, Compression::default());
        encoder
            .write_all(ori)
            .map_err(|e| anyhow::anyhow!("Gzip compression write failed: {}", e))?;
        encoder
            .finish()
            .map_err(|e| anyhow::anyhow!("Gzip compression finish failed: {}", e))?;
        Ok(compressed_data)
    }

    pub(crate) fn de_compress(ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut decompressed_data = Vec::new();
        let mut decoder = GzDecoder::new(ori);
        decoder
            .read_to_end(&mut decompressed_data)
            .map_err(|e| anyhow::anyhow!("Gzip decompression failed: {}", e))?;
        Ok(decompressed_data)
    }
}
