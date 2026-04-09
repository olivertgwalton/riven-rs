use riven_rank::parse;

#[test]
fn test_hdr_dolby_vision() {
    let data = parse("Movie.Title.2160p.DV.BluRay");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_hdr10_plus() {
    let data = parse("Movie.Title.2160p.HDR10+.BluRay");
    assert!(data.hdr.contains(&"HDR10+".to_string()));
}

#[test]
fn test_hdr_hdr() {
    let data = parse("Movie.Title.2160p.HDR.BluRay");
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_sdr() {
    let data = parse("Movie.Title.1080p.SDR.BluRay");
    assert!(data.hdr.contains(&"SDR".to_string()));
}

#[test]
fn test_hdr_dv_and_hdr() {
    let data = parse("Movie.Title.2160p.DV.HDR.BluRay");
    assert!(data.hdr.contains(&"DV".to_string()));
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_basic() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_hdr10() {
    let data = parse(
        "Spider-Man - Complete Movie Collection (2002-2022) 1080p.HEVC.HDR10.1920x800.x265. DTS-HD",
    );
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_hdr10plus() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert!(data.hdr.contains(&"HDR10+".to_string()));
    assert!(!data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_dolby_vision_dv() {
    let data = parse("Belle (2021) 2160p 10bit 4KLight DOLBY VISION BluRay DDP 7.1 x265-QTZ");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_dv_from_dovi() {
    let data = parse("Bullet.Train.2022.2160p.WEB-DL.DoVi.DD5.1.HEVC-EVO[TGx]");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_empty_when_absent() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(data.hdr.is_empty());
}

#[test]
fn test_hdr_dv_from_dolby_vision_text() {
    let data = parse(
        "Андор / Andor [01x01-03 из 12] (2022) WEB-DL-HEVC 2160p | 4K | Dolby Vision TV | NewComers, HDRezka Studio",
    );
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_hdr10_plus_with_keyword() {
    let data = parse("Movie 2022 2160p WEB-DL HDR10Plus DDP5 1-GROUP");
    assert!(data.hdr.contains(&"HDR10+".to_string()));
}
