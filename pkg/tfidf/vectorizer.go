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

package tfidf

import (
	"math"
	"strings"
)

// Vectorizer represents a TF-IDF vectorizer
type Vectorizer struct {
	vocabulary map[string]int
	idf        []float64
}

// NewVectorizer creates a new Vectorizer
func NewVectorizer() *Vectorizer {
	return &Vectorizer{
		vocabulary: make(map[string]int),
	}
}

// Fit fits the vectorizer to the input documents
func (v *Vectorizer) Fit(docs []string) {
	docCount := len(docs)
	termDocCount := make(map[string]int)

	for _, doc := range docs {
		terms := strings.Fields(doc)
		seen := make(map[string]bool)
		for _, term := range terms {
			term = strings.ToLower(term)
			if _, exists := v.vocabulary[term]; !exists {
				v.vocabulary[term] = len(v.vocabulary)
			}
			if !seen[term] {
				termDocCount[term]++
				seen[term] = true
			}
		}
	}

	v.idf = make([]float64, len(v.vocabulary))
	for term, count := range termDocCount {
		v.idf[v.vocabulary[term]] = math.Log(float64(docCount) / float64(count+1))
	}
}

// Transform transforms the input documents to TF-IDF vectors
func (v *Vectorizer) Transform(docs []string) [][]float64 {
	tfIdfVectors := make([][]float64, len(docs))

	for i, doc := range docs {
		terms := strings.Fields(doc)
		tf := make([]float64, len(v.vocabulary))

		for _, term := range terms {
			term = strings.ToLower(term)
			tf[v.vocabulary[term]]++
		}

		tfIdf := make([]float64, len(v.vocabulary))
		for j, termFreq := range tf {
			tfIdf[j] = termFreq * v.idf[j]
		}

		tfIdfVectors[i] = tfIdf
	}

	return tfIdfVectors
}

// FitTransform fits the vectorizer to the input documents and then transforms them
func (v *Vectorizer) FitTransform(docs []string) [][]float64 {
	v.Fit(docs)
	return v.Transform(docs)
}

