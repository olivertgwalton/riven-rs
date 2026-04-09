use riven_rank::parse;

#[test]
fn test_ppv() {
    let data = parse("UFC 287 PPV 720p HDTV");
    assert!(data.ppv);
}

#[test]
fn test_ppv_fight_night() {
    let data = parse("UFC Fight Night 720p HDTV");
    assert!(data.ppv);
}

#[test]
fn test_ptt_sports_ufc_239() {
    let data = parse("UFC.239.PPV.Jones.Vs.Santos.HDTV.x264-PUNCH[TGx]");
    assert_eq!(data.parsed_title, "UFC 239 Jones Vs Santos");
    assert_eq!(data.seasons, Vec::<i32>::new());
    assert_eq!(data.episodes, Vec::<i32>::new());
    assert_eq!(data.languages, Vec::<String>::new());
    assert_eq!(data.quality, Some("HDTV".into()));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.group, Some("PUNCH".into()));
    assert!(data.ppv);
}

#[test]
fn test_ptt_sports_ufc_fight_night() {
    let data = parse("UFC.Fight.Night.158.Cowboy.vs.Gaethje.WEB.x264-PUNCH[TGx]");
    assert_eq!(data.parsed_title, "UFC Fight Night 158 Cowboy vs Gaethje");
    assert_eq!(data.quality, Some("WEB".into()));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.group, Some("PUNCH".into()));
    assert!(data.ppv);
}
