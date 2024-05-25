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
