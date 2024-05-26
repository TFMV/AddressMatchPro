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
	"encoding/gob"
	"log"
	"math"
	"os"
	"strings"

	"github.com/TFMV/FuzzyMatchFinder/internal/standardizer"
)

// Scorer represents a machine learning model for scoring candidates.
type Scorer struct {
	Model *LogisticRegression
}

// LogisticRegression is a simple logistic regression model.
type LogisticRegression struct {
	Coef      []float64
	Intercept float64
}

// LoadModel loads a pre-trained logistic regression model from a file.
func LoadModel(filename string) (*Scorer, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var model LogisticRegression
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&model); err != nil {
		return nil, err
	}

	return &Scorer{Model: &model}, nil
}

// Score calculates a match score for the given features using the loaded model.
func (s *Scorer) Score(features []float64) float64 {
	return logistic(dotProduct(s.Model.Coef, features) + s.Model.Intercept)
}

// dotProduct calculates the dot product of two vectors.
func dotProduct(vec1, vec2 []float64) float64 {
	var result float64
	for i, v := range vec1 {
		result += v * vec2[i]
	}
	return result
}

// logistic applies the logistic function to the given value.
func logistic(x float64) float64 {
	return 1 / (1 + math.Exp(-x))
}

// ExtractFeatures extracts features from the candidate and request for scoring.
func ExtractFeatures(req MatchRequest, candidate Candidate, standardizedCandidateAddress string) []float64 {
	var features []float64

	// Example feature: name similarity
	fullNameReq := strings.ToLower(strings.TrimSpace(req.FirstName + " " + req.LastName))
	fullNameCand := strings.ToLower(strings.TrimSpace(candidate.FullName))
	nameSimilarity := ngramFrequencySimilarity(fullNameReq, fullNameCand, 3)
	features = append(features, nameSimilarity)

	// Example feature: phone number similarity
	phoneSimilarity := 0.0
	if req.PhoneNumber == candidate.FullName {
		phoneSimilarity = 1.0
	}
	features = append(features, phoneSimilarity)

	// Example feature: address similarity
	standardizedReqAddress, err := standardizer.StandardizeAddress(req.Street)
	if err != nil {
		log.Printf("Failed to standardize request address: %v\n", err)
	}
	addressSimilarity := ngramFrequencySimilarity(standardizedReqAddress, standardizedCandidateAddress, 3)
	features = append(features, addressSimilarity)

	// Add more features as needed

	return features
}
