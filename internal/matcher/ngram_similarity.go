// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

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
