#[test]
fn test_rotate_left_and_right() {
    let h: u32 = 12345678;
    let delta1 = (h >> 17) | (h << 15);
    println!("delta1:{}", delta1);

    let delta2 = h.rotate_right(17) | h.rotate_left(15);
    println!("delta1:{}", delta2);
}

#[test]
fn test_div_ceil() {
    let nbits: u32 = 0;
    let nbytes1 = (nbits + 7) / 8;

    println!("nbytes:{}", nbytes1);

    let nbytes2 = nbits.div_ceil(8);
    println!("nbytes:{}", nbytes2);
}
