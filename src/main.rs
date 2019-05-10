extern crate memmap;
extern crate rayon;

use rayon::prelude::*;
use memmap::MmapOptions;
use std::collections::HashMap;
use std::fs::File;
use std::str;

// I don't know how well HashMap deals with iterating over very small
// maps, but I'm pretty sure it's not optimized for that purpose.
// Certainly special-casing singleton maps turns out to give us a
// small but nice performance boost.
enum Tally<'k> {
    One(&'k [u8], u32),
    Many(HashMap<&'k [u8], u32>)
}

fn merge<'k>(a: Tally<'k>, b: Tally<'k>) -> Tally<'k> {
    use Tally::*;
    match (a, b) {
        (One(ak, av), One(bk, bv)) => {
            if ak == bk {
                One(ak, av + bv)
            } else {
                let mut r = HashMap::new();
                r.insert(ak, av);
                r.insert(bk, bv);
                Many(r)
            }
        },
        (One(ak, av), Many(mut bbs)) => Many({*bbs.entry(ak).or_insert(0) += av; bbs}),
        (Many(mut aas), One(bk, bv)) => Many({*aas.entry(bk).or_insert(0) += bv; aas}),
        (Many(aas), Many(bbs)) => {
            let (mut small, mut large) = if aas.len() > bbs.len() { (bbs, aas) } else { (aas, bbs) };
            for (k, v) in small.into_iter() {
                *large.entry(k).or_insert(0) += v;
            }
            Many(large)
        }
    }
}

fn main() {
    let file = File::open("itcont.txt").unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

    if let Tally::Many(tally) = &mmap.par_split(|c| *c == '\n' as u8)
        .map(tally_line)
        .reduce(|| Tally::Many(HashMap::new()), merge) {
            let best = tally.iter().max_by_key(|(_name, count)| *count);
            if let Some((best_name, best_count)) = best {
                println!("{:?}: {:?}", str::from_utf8(best_name).unwrap(), best_count);
            }

        }
}

fn find_name<'a>(line: &'a [u8]) -> Option<&'a [u8]> {
    let name_part = line.split(|c| *c == '|' as u8).nth(7)?;
    let first_name_part = name_part.split(|c| *c == ',' as u8).nth(1)?;
    // Sometimes people don't put a space after the comma. Too bad for them.
    first_name_part.split(|c| *c == ' ' as u8 || *c == '|' as u8).nth(1)
}

fn tally_line<'a>(line: &'a [u8]) -> Tally<'a> {
    if let Some(name) = find_name(line) {
        Tally::One(name, 1)
    } else {
        Tally::Many(HashMap::new())
    }
}
