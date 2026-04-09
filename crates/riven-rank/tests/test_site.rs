use riven_rank::parse;

#[test]
fn test_site_detected() {
    let data = parse(
        "www.1TamilMV.world - Ayalaan (2024) Tamil PreDVD - 1080p - x264 - HQ Clean Aud - 2.5GB.mkv",
    );
    assert_eq!(data.site, Some("www.1TamilMV.world".to_string()));
}

#[test]
fn test_site_torrenting() {
    let data = parse("www.Torrenting.com   -    Anatomy Of A Fall (2023)");
    assert_eq!(data.site, Some("www.Torrenting.com".to_string()));
}

#[test]
fn test_site_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.site, None);
}

#[test]
fn test_ptt_site_corpus_sample() {
    let cases = [
        (
            "The.Expanse.S05E02.1080p.AMZN.WEB.DDP5.1.x264-NTb[eztv.re].mp4",
            Some("eztv.re"),
        ),
        (
            "www.1TamilMV.world - Raja Vikramarka (2024) Tamil HQ HDRip - 400MB - x264 - AAC - ESub.mkv",
            Some("www.1TamilMV.world"),
        ),
        (
            "[HD-ELITE.NET] -  The.Art.Of.The.Steal.2014.DVDRip.XviD.Dual.Aud",
            Some("HD-ELITE.NET"),
        ),
        (
            "Last.Call.for.Istanbul.2023.1080p.NF.WEB-DL.DDP5.1.H.264.MKV.torrent",
            None,
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.site.as_deref(), expected, "{input}");
    }
}

#[test]
fn test_ptt_site_corpus_sample_direct() {
    let cases = [
        (
            "The.Expanse.S05E02.1080p.AMZN.WEB.DDP5.1.x264-NTb[eztv.re].mp4",
            Some("eztv.re"),
        ),
        (
            "www.1TamilBlasters.lat - Thuritham (2023) [Tamil - 2K QHD AVC UNTOUCHED - x264 - AAC - 3.4GB - ESub].mkv",
            Some("www.1TamilBlasters.lat"),
        ),
        (
            "www.1TamilMV.world - Raja Vikramarka (2024) Tamil HQ HDRip - 400MB - x264 - AAC - ESub.mkv",
            Some("www.1TamilMV.world"),
        ),
        (
            "Anatomia De Grey - Temporada 19 [HDTV][Cap.1905][Castellano][www.AtomoHD.nu].avi",
            Some("www.AtomoHD.nu"),
        ),
        (
            "[HD-ELITE.NET] -  The.Art.Of.The.Steal.2014.DVDRip.XviD.Dual.Aud",
            Some("HD-ELITE.NET"),
        ),
        (
            "[ Torrent9.cz ] The.InBetween.S01E10.FiNAL.HDTV.XviD-EXTREME.avi",
            Some("Torrent9.cz"),
        ),
        (
            "Jurassic.World.Dominion.CUSTOM.EXTENDED.2022.2160p.MULTi.VF2.UHD.Blu-ray.REMUX.HDR.DoVi.HEVC.DTS-X.DTS-HDHRA.7.1-MOONLY.mkv",
            None,
        ),
        (
            "[Naruto-Kun.Hu] Naruto - 061 [1080p].mkv",
            Some("Naruto-Kun.Hu"),
        ),
        (
            "www 1TamilMV ms - The Electric State (2025) HQ HDRip - x264 - [Tam + Tel + Hin] - AAC - 450MB - ESub mkv",
            Some("www 1TamilMV ms"),
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.site.as_deref(), expected, "{raw}");
    }
}

#[test]
fn test_ptt_site_corpus_sample_direct_more() {
    let cases = [
        (
            "Last.Call.for.Istanbul.2023.1080p.NF.WEB-DL.DDP5.1.H.264.MKV.torrent",
            None,
        ),
        (
            "www 1TamilBlasters rodeo - The Electric State (2025) [1080p HQ HD AVC - x264 - [Tam + Tel + Hin + Eng(ATMOS)] - DDP5 1(640Kbps) - 6 6GB - ESub] mkv",
            Some("www 1TamilBlasters rodeo"),
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.site.as_deref(), expected, "{raw}");
    }
}
