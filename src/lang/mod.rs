// TODO: Tokenize, stem, lemmatize, remove stop words

use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split_whitespace()
        .map(|s| {
            s.chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

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

/// Generate a token blacklist from the index
///
/// This is used to remove tokens that are too common, such as "the", "and", "is", etc,
/// therefore not adding much value to the search and increasing the size of the index.
///
/// This function iterates over the index and collects all the tokens, then filters out the most common (top 1%)
/// and returns them as a blacklist. It does not run if there are less than 1000 tokens in the index.
///
/// There may be a more sophisticated approach to this in the future, but for now this is a simple solution.
// TODO: When to run it? Need some kind of scheduler for this.
// async fn generate_token_blacklist(engine: &SearchEngine) -> Vec<String> {
//     unimplemented!();
//     // let mut blacklist = HashSet::new();
//     // for bucket in engine.index.iter() {
//     //     for collection in bucket.iter() {
//     //         for token in collection.iter() {
//     //             blacklist.insert(token.key().to_string());
//     //         }
//     //     }
//     // }
//     // blacklist.into_iter().collect()
// }
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple() {
        let text = "Hello, World!";
        let tokens = tokenize(text);
        assert_eq!(tokens, ["hello", "world"]);
    }

    #[test]
    fn test_tokenize_lowercase() {
        let text = "Hello, World!";
        let tokens = tokenize(text);
        assert_eq!(tokens, ["hello", "world"]);
    }
    #[test]
    fn test_tokenize_other_alphabets() {
        let text = "Hello, World! こんにちは! Привет, мир!";
        let tokens = tokenize(text);
        assert_eq!(tokens, ["hello", "world", "こんにちは", "привет", "мир"]);
    }
}
