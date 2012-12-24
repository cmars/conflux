package conflux

import (
	"github.com/bmizerany/assert"
	"math/big"
	"testing"
)

const TEST_MATRIX_SIZE = 5

func TestMatrixPutGet(t *testing.T) {
	p := big.NewInt(int64(65537))
	m := NewMatrix(TEST_MATRIX_SIZE, TEST_MATRIX_SIZE, Zi(p, 23))
	m.Set(2, 2, Zi(p, 24))
	n := 0
	for i := 0; i < TEST_MATRIX_SIZE; i++ {
		for j := 0; j < TEST_MATRIX_SIZE; j++ {
			n++
			if i == 2 && j == 2 {
				assert.Equal(t, int64(24), m.Get(i, j).Int64())
			} else {
				assert.Equal(t, int64(23), m.Get(i, j).Int64())
			}
		}
	}
	assert.Equal(t, 25, n)
}
