fn main() {
    let title = "Game of Thrones S01E01 1080p";
    let p = riven_rank::parse(title);
    println!("{}: {:#?}", title, p);
    
    let title2 = "ShowName S01";
    let p2 = riven_rank::parse(title2);
    println!("{}: {:#?}", title2, p2);
}
