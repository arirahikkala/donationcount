extern crate memmap;
extern crate rayon;

use rayon::prelude::*;
use memmap::MmapOptions;
use std::collections::HashMap;
use std::fs::File;
use std::str;

fn main() {
    let file = File::open("itcont.txt").unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

    let tally = make_chunks(&mmap).par_iter()
        .map(|chunk| process_chunk(chunk))
        .reduce(|| HashMap::new(), merge_tallies);

    let mut best_count = 0;
    let mut best_name = None;
    for (name, count) in tally.iter() {
        if *count > best_count {
            best_name = Some(name);
            best_count = *count;
        }
    }
    if let Some(best) = best_name {
        println!("{:?}: {:?}", str::from_utf8(best).unwrap(), best_count);
    }
}

// divide file into slightly-bigger-than-10-megabyte chunks
// delimited by newlines
fn make_chunks(file: &[u8]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    let mut last_chunk_end = 0;
    let mut this_chunk_end = 0;
    loop {
        this_chunk_end += 10 * 1024 * 1024;
        if this_chunk_end >= file.len() {
            chunks.push(&file[last_chunk_end..]);
            break;
        } else {
            if let Some(line_end) = find_start_of_record(1, |c| c == '\n' as u8, &file[this_chunk_end..]) {
                this_chunk_end += line_end;
                chunks.push(&file[last_chunk_end..this_chunk_end]);
                last_chunk_end = this_chunk_end;
            } else {
                chunks.push(&file[last_chunk_end..]);
                break;
            }
        }
    }
    chunks
}

fn merge_tallies<'k>(mut a: HashMap<&'k [u8], u32>, b: HashMap<&'k [u8], u32>) -> HashMap<&'k [u8], u32> {
    for (name, count) in b.iter() {
        *a.entry(name).or_insert(0) += count;
    }
    a
}

fn find_start_of_record<F>(record_index: u32, is_record_delimiter: F, line: &[u8]) -> Option<usize>
    where F: Fn(u8) -> bool {
    let mut seen_records = 0;
    for (i, c) in line.iter().enumerate() {
        if seen_records == record_index {
            return Some(i);
        }
        if is_record_delimiter(*c) {
            seen_records += 1;
        }
    }
    return None;
}

fn process_line<'a>(tally : &mut HashMap<&'a [u8], u32>, line : &'a [u8]) {
    let record_7_index = find_start_of_record(7, |c| c == '|' as u8, line).unwrap();
    if let Some(name_index) = find_start_of_record(1, |c| c == ',' as u8, &line[record_7_index..line.len()]) {
        let name_start = record_7_index + name_index + 1;
        if let Some(name_end) = find_start_of_record(1, |c| c == ' ' as u8 || c == '|' as u8, &line[name_start+1..line.len()]) {
            let name = &line[name_start..name_start + name_end];
            *tally.entry(name).or_insert(0) += 1;
        }
    }
}

fn process_chunk<'a>(chunk: &'a [u8]) -> HashMap<&'a [u8], u32> {
    let mut tally = HashMap::new();

    let mut last_start = 0;
    for (i, c) in chunk.iter().enumerate() {
        if *c == '\n' as u8 {
            process_line(&mut tally, &chunk[last_start..i]);
            last_start = i + 1;
        }
    }
    return tally;
}
