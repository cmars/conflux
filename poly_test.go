package conflux

import (
	"github.com/bmizerany/assert"
	"math/big"
	"testing"
)

func TestPolyDegree(t *testing.T) {
	p := big.NewInt(int64(65537))
	poly := NewPoly(Zi(p, 4), Zi(p, 3), Zi(p, 2))
	assert.Equal(t, 2, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, nil)
	assert.Equal(t, 0, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, Zi(p, 1))
	assert.Equal(t, 7, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, Zi(p, 1), nil, nil)
	assert.Equal(t, 7, poly.Degree())
}

func TestPolyFmt(t *testing.T) {
	p := big.NewInt(int64(65537))
	poly := NewPoly(Zi(p, 4), Zi(p, 3), Zi(p, 2))
	assert.Equal(t, "2z^2 + 3z^1 + 4", poly.String())
}
