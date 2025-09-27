use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            let batch_size = rbuf.get_u32() as usize;
            if rbuf.remaining() < batch_size {
                bail!("incomplete WAL.");
            }
            let mut batch_buf = &rbuf[..batch_size];
            let mut kv_pairs = Vec::new();
            let mut hasher = crc32fast::Hasher::new();

            let single_checksum = crc32fast::hash(batch_buf);

            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16() as usize;
                hasher.write(&(key_len as u16).to_be_bytes());
                let key = Bytes::copy_from_slice(&batch_buf[..key_len]);
                hasher.write(&key);
                batch_buf.advance(key_len);
                let ts = batch_buf.get_u64();
                hasher.write(&ts.to_be_bytes());
                let key_type_byte = batch_buf.get_u8();
                hasher.write(&[key_type_byte]);
                let ttl = batch_buf.get_u64();
                hasher.write(&ttl.to_be_bytes());
                let value_len = batch_buf.get_u16() as usize;
                hasher.write(&(value_len as u16).to_be_bytes());
                let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
                hasher.write(&value);
                kv_pairs.push((key, ts, key_type_byte, ttl, value));
                batch_buf.advance(value_len);
            }
            rbuf.advance(batch_size);
            let expected_checksum = rbuf.get_u32();
            let component_checksum = hasher.finalize();
            assert_eq!(component_checksum, single_checksum);
            if single_checksum != expected_checksum {
                bail!("checksum mismatch");
            }
            for (key, ts, key_type_byte, ttl, value) in kv_pairs {
                let key_type =
                    crate::key::Type::try_from(key_type_byte).unwrap_or(crate::key::Type::PUT);
                let key_bytes = KeyBytes::from_bytes_with_all(key, ts, key_type, ttl);
                skiplist.insert(key_bytes, value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::<u8>::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u8(u8::from(key.key_type()));
            buf.put_u64(key.ttl());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        file.write_all(&(buf.len() as u32).to_be_bytes())?;
        file.write_all(&buf)?;
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
        Ok(())
    }
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
