// TODO: Tokenize, stem, lemmatize, remove stop words

use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

#[inline(never)]
pub fn tokenize_iter(text: &mut String) -> impl Iterator<Item = &str> {
    text.to_lowercase();
    text.split_whitespace()
}

fn cleanup_useless_tokens(tokens: Vec<String>) -> Vec<String> {
    let word_blacklist = [
        "the", "and", "is", "are", "was", "were", "have", "has", "had", "do", "does", "did",
    ];
    tokens
        .into_par_iter()
        .filter(|token| !word_blacklist.contains(&token.as_str()))
        .collect()
}
