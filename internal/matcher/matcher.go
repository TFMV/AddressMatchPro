package matcher

import (
	"math"
	"strings"
)

// Compute cosine similarity between two texts
func CosineSimilarity(text1, text2 string) float64 {
	vec1 := textToVector(text1)
	vec2 := textToVector(text2)
	return dotProduct(vec1, vec2) / (magnitude(vec1) * magnitude(vec2))
}

func textToVector(text string) map[string]float64 {
	vector := make(map[string]float64)
	words := strings.Fields(text)
	for _, word := range words {
		vector[word]++
	}
	return vector
}

func dotProduct(vec1, vec2 map[string]float64) float64 {
	var result float64
	for key, value := range vec1 {
		if value2, found := vec2[key]; found {
			result += value * value2
		}
	}
	return result
}

func magnitude(vec map[string]float64) float64 {
	var result float64
	for _, value := range vec {
		result += value * value
	}
	return math.Sqrt(result)
}

// ExtractFeatures extracts features from the candidate and request for scoring.
func ExtractFeatures(req MatchRequest, candidate Candidate) []float64 {
	// Example feature extraction: [cosine similarity, ...]
	return []float64{
		CosineSimilarity(req.FirstName, candidate.FullName),
		CosineSimilarity(req.LastName, candidate.FullName),
		CosineSimilarity(req.PhoneNumber, candidate.PhoneNumber),
		CosineSimilarity(req.Street, candidate.Street),
		CosineSimilarity(req.City, candidate.City),
		CosineSimilarity(req.State, candidate.State),
		CosineSimilarity(req.ZipCode, candidate.ZipCode),
	}
}
