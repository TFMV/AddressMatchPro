package matcher

import (
	"math"
	"strings"
	"unicode"
)

// Tokenize a string into n-grams
func ngrams(s string, n int) []string {
	normalized := normalizeString(s)
	ngrams := make([]string, 0, len(normalized)-n+1)
	for i := 0; i <= len(normalized)-n; i++ {
		ngrams = append(ngrams, normalized[i:i+n])
	}
	return ngrams
}

// Normalize string by removing punctuation and converting to lowercase
func normalizeString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// Calculate cosine similarity between two sets of n-grams
func cosineSimilarity(a, b []string) float64 {
	ngramFreqA := getNgramFrequencies(a)
	ngramFreqB := getNgramFrequencies(b)

	var dotProduct, magA, magB float64
	for k, vA := range ngramFreqA {
		vB, found := ngramFreqB[k]
		if found {
			dotProduct += float64(vA * vB)
		}
		magA += float64(vA * vA)
	}
	for _, vB := range ngramFreqB {
		magB += float64(vB * vB)
	}

	if magA == 0 || magB == 0 {
		return 0.0
	}

	return dotProduct / (math.Sqrt(magA) * math.Sqrt(magB))
}

// Get n-gram frequencies from a list of n-grams
func getNgramFrequencies(ngrams []string) map[string]int {
	freq := make(map[string]int, len(ngrams))
	for _, ngram := range ngrams {
		freq[ngram]++
	}
	return freq
}

// Calculate n-gram frequency similarity between two strings
func ngramFrequencySimilarity(s1, s2 string, n int) float64 {
	ngramsA := ngrams(s1, n)
	ngramsB := ngrams(s2, n)
	return cosineSimilarity(ngramsA, ngramsB)
}
