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

func TestPolyEval(t *testing.T) {
	var poly *Poly
	var z *Zp
	p := big.NewInt(int64(97))
	// Constant
	poly = NewPoly(Zi(p, 5))
	z = poly.Eval(Zi(p, 8))
	assert.Equal(t, int64(5), z.Int64())
	// Linear
	poly = NewPoly(Zi(p, 5), Zi(p, 3))
	z = poly.Eval(Zi(p, 8))
	assert.Equal(t, int64(29), z.Int64())
	// Quadratic
	poly = NewPoly(Zi(p, 5), Zi(p, 3), Zi(p, 2))
	z = poly.Eval(Zi(p, 8))
	assert.Equal(t, Zi(p, 157).Int64(), z.Int64())
}

func TestPolyDivmod(t *testing.T) {
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, -6), Zi(p, 11), Zi(p, -6), Zi(p, 1))
	y := NewPoly(Zi(p, 2), Zi(p, 1))
	q, r, err := PolyDivmod(x, y)
	assert.Equal(t, err, nil)
	t.Logf("q=%v r=%v", q, r)
}

