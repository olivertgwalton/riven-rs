use riven_rank::parse;

#[test]
fn test_year() {
    let data = parse("Movie.Title.2023.1080p.BluRay");
    assert_eq!(data.year, Some(2023));
}

#[test]
fn test_year_in_brackets() {
    let data = parse("Movie Title (2019) 1080p BluRay");
    assert_eq!(data.year, Some(2019));
}

#[test]
fn test_year_range_complete() {
    let data = parse("Show Title 2000-2005 DVDRip");
    assert_eq!(data.year, Some(2000));
    assert!(data.complete);
}

#[test]
fn test_year_standard() {
    let data = parse("Dawn.of.the.Planet.of.the.Apes.2014.HDRip.XViD-EVO");
    assert_eq!(data.year, Some(2014));
}

#[test]
fn test_year_in_parens() {
    let data = parse("Hercules (2014) 1080p BrRip H264 - YIFY");
    assert_eq!(data.year, Some(2014));
}

#[test]
fn test_year_recent() {
    let data = parse("Oppenheimer.2023.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv");
    assert_eq!(data.year, Some(2023));
}

#[test]
fn test_year_1988() {
    let data = parse("Rain Man 1988 REMASTERED 1080p BRRip x264 AAC-m2g");
    assert_eq!(data.year, Some(1988));
}

#[test]
fn test_year_2022() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert_eq!(data.year, Some(2022));
}

#[test]
fn test_year_2019() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.year, Some(2019));
}

#[test]
fn test_year_2021() {
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.year, Some(2021));
}

#[test]
fn test_year_2015() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.year, Some(2015));
}

#[test]
fn test_year_none_when_absent() {
    let data = parse("Movie.Title.BluRay.x264-GROUP");
    assert_eq!(data.year, None);
}
