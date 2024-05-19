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
