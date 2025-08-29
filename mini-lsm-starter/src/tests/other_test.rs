use bytes::BufMut;
use std::hash::Hasher;

#[test]
pub fn vec_capacity_test() {
    let key = "kejkjkhkjhy".as_bytes();
    let value = "vaghjgjgjgjlue".as_bytes();

    let mut buf: Vec<u8> = Vec::with_capacity(
        key.len() + value.len() + 2 * std::mem::size_of::<u16>() + std::mem::size_of::<u32>(),
    );

    let mut buf: Vec<u8> = Vec::with_capacity(key.len() + value.len() + std::mem::size_of::<u16>());

    print!("buf size:{}\n", buf.len());
    print!("buf capacity:{}\n", buf.capacity());

    let mut hasher = crc32fast::Hasher::new();
    hasher.write_u16(key.len() as u16);
    buf.put_u16(key.len() as u16);
    hasher.write(key);
    buf.put_slice(key);
    hasher.write_u16(value.len() as u16);
    buf.put_u16(value.len() as u16);
    buf.put_slice(value);
    hasher.write(value);
    // add checksum: week 2 day 7
    buf.put_u32(hasher.finalize());

    print!("buf size:{}\n", buf.len());
    print!("buf capacity:{}\n", buf.capacity());
}
