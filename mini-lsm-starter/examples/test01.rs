fn main() {
    pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
    // 2 字节
    println!("{}", SIZEOF_U16);
}
