use crate::compression::gz::GzCompressionController;
use crate::compression::lz4::Lz4CompressionController;
use crate::compression::snappy::SnappyCompressionController;

pub(crate) mod gz;
pub(crate) mod lz4;
pub(crate) mod snappy;

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum CompressionOptions {
    None = 0,
    Snappy = 1,
    Lz4 = 2,
    Gz = 3,
}

impl From<CompressionOptions> for u8 {
    fn from(value: CompressionOptions) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for CompressionOptions {
    type Error = &'static str; // 错误类型

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionOptions::None),
            1 => Ok(CompressionOptions::Snappy),
            2 => Ok(CompressionOptions::Lz4),
            3 => Ok(CompressionOptions::Gz),
            _ => Err("Invalid compression type value"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompressionController {
    Snappy,
    Lz4,
    Gz,
    None,
}

impl CompressionController {
    pub fn new(compression_options: CompressionOptions) -> Self {
        match compression_options {
            CompressionOptions::Snappy => CompressionController::Snappy,
            CompressionOptions::Lz4 => CompressionController::Lz4,
            CompressionOptions::Gz => CompressionController::Gz,
            _ => CompressionController::None,
        }
    }

    pub fn compress(&self, ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            CompressionController::Snappy => SnappyCompressionController::compress(ori),
            CompressionController::Lz4 => Lz4CompressionController::compress(ori),
            CompressionController::Gz => GzCompressionController::compress(ori),
            _ => Ok(Vec::from(ori)),
        }
    }

    pub fn de_compress(&self, ori: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            CompressionController::Snappy => SnappyCompressionController::de_compress(ori),
            CompressionController::Lz4 => Lz4CompressionController::de_compress(ori),
            CompressionController::Gz => GzCompressionController::de_compress(ori),
            _ => Ok(ori.to_vec()),
        }
    }
}
