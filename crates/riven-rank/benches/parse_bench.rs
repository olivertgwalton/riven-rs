use criterion::{Criterion, black_box, criterion_group, criterion_main};
use riven_rank::parse;

const RELEASES: &[&str] = &[
    "sons.of.anarchy.s05e10.480p.BluRay.x264-GAnGSteR",
    "Avengers.Endgame.2019.2160p.UHD.BluRay.REMUX.HDR.HEVC.Atmos-EPSiLON",
    "[SubsPlease] One Piece - 1111 (480p) [2E05E658].mkv",
    "[OFFICIAL ENG SUB] Soul Land Episode 121-125 [1080p][Soft Sub][Web-DL][Douluo Dalu][斗罗大陆]",
    "www.MovCr.to - Bikram Yogi, Guru, Predator (2019) 720p WEB_DL x264 ESubs [Dual Audio]-[Hindi + Eng] - 950MB - MovCr.mkv",
    "Dune.Part.Two.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX[TGx]",
    "[www.1TamilMV.pics]_The.Great.Indian.Suicide.2023.Tamil.TRUE.WEB-DL.4K.SDR.HEVC.(DD+5.1.384Kbps.&.AAC).3.2GB.ESub.mkv",
    "The.Office.US.S01-09.COMPLETE.SERIES.1080P.BLURAY.X265-HIQVE",
    "BoJack Horseman [06x01-08 of 16] (2019-2020) WEB-DLRip 720p",
    "Wonder.Woman.1984.2020.3D.1080p.BluRay.x264-SURCODE[rarbg]",
    "Godzilla.x.Kong.The.New.Empire.2024.2160p.BluRay.REMUX.DV.P7.HDR.ENG.LATINO.GER.ITA.FRE.HINDI.CHINESE.TrueHD.Atmos.7.1.H265-BEN.THE.MEN",
    "Futurama.S08E03.How.the.West.Was.1010001.1080p.HULU.WEB-DL.DDP5.1.H.264-FLUX.mkv",
];

fn parse_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse");

    group.bench_function("single_mixed_corpus", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let raw = RELEASES[idx % RELEASES.len()];
            idx += 1;
            black_box(parse(black_box(raw)))
        });
    });

    group.bench_function("batch_mixed_corpus", |b| {
        b.iter(|| {
            for raw in RELEASES {
                black_box(parse(black_box(raw)));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, parse_benchmark);
criterion_main!(benches);
