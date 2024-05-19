package pca

import (
	"gonum.org/v1/gonum/mat"
)

type PCA struct {
	NumComponents int
	svd           *mat.SVD
}

// NewPCA creates a new PCA instance with the specified number of components.
func NewPCA(numComponents int) *PCA {
	return &PCA{NumComponents: numComponents}
}

// FitTransform fits the PCA model to the data and transforms it.
func (pca *PCA) FitTransform(X *mat.Dense) *mat.Dense {
	return pca.Fit(X).Transform(X)
}

// Fit fits the PCA model to the data.
func (pca *PCA) Fit(X *mat.Dense) *PCA {
	// Mean center the input data
	M := mean(X)
	X = matrixSubVector(X, M)

	// Perform SVD
	pca.svd = &mat.SVD{}
	ok := pca.svd.Factorize(X, mat.SVDThin)
	if !ok {
		panic("Unable to factorize")
	}
	if pca.NumComponents < 0 {
		panic("Number of components can't be less than zero")
	}

	return pca
}

// Transform transforms the data using the fitted PCA model.
func (pca *PCA) Transform(X *mat.Dense) *mat.Dense {
	if pca.svd == nil {
		panic("You should fit the PCA model first")
	}

	numSamples, numFeatures := X.Dims()

	var vTemp mat.Dense
	pca.svd.VTo(&vTemp)
	// Compute the full data
	if pca.NumComponents == 0 || pca.NumComponents > numFeatures {
		return compute(X, &vTemp)
	}

	X = compute(X, &vTemp)
	result := mat.NewDense(numSamples, pca.NumComponents, nil)
	result.Copy(X)
	return result
}

// Helper functions

// mean computes the mean of the columns of the input matrix.
func mean(matrix *mat.Dense) *mat.Dense {
	rows, cols := matrix.Dims()
	meanVector := make([]float64, cols)
	for i := 0; i < cols; i++ {
		sum := mat.Sum(matrix.ColView(i))
		meanVector[i] = sum / float64(rows)
	}
	return mat.NewDense(1, cols, meanVector)
}

// matrixSubVector subtracts the mean vector from the input matrix.
func matrixSubVector(mat, vec *mat.Dense) *mat.Dense {
	rowsm, colsm := mat.Dims()
	_, colsv := vec.Dims()
	if colsv != colsm {
		panic("Error in dimension")
	}
	for i := 0; i < rowsm; i++ {
		for j := 0; j < colsm; j++ {
			mat.Set(i, j, (mat.At(i, j) - vec.At(0, j)))
		}
	}
	return mat
}

// compute multiplies the input matrix X by the matrix Y.
func compute(X, Y mat.Matrix) *mat.Dense {
	var ret mat.Dense
	ret.Mul(X, Y)
	return &ret
}
