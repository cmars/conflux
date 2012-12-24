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

func TestPolyMul(t *testing.T) {
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, -6), Zi(p, 11), Zi(p, -6), Zi(p, 1))
	y := NewPoly(Zi(p, 2), Zi(p, 1))
	z := NewPoly().Mul(x, y)
	assert.Equal(t, 5, len(z.coeff))
	t.Logf("z=%v", z)
	for i, v := range []int{85, 16, 96, 93, 1} {
		assert.Equal(t, Zi(p, v).String(), z.coeff[i].String())
	}
}

func TestPolyAdd(t *testing.T) {
	p := big.NewInt(int64(97))
	// (x+1) + (x+2) = (2x+3)
	x := NewPoly(Zi(p, 1), Zi(p, 1))
	y := NewPoly(Zi(p, 2), Zi(p, 1))
	z := NewPoly().Add(x, y)
	assert.Equal(t, 1, z.degree)
	assert.Equal(t, int64(3), z.coeff[0].Int64())
	assert.Equal(t, int64(2), z.coeff[1].Int64())
	// (2x+3) - (x+2) = (x+1)
	x = NewPoly(Zi(p, 3), Zi(p, 2))
	y = NewPoly(Zi(p, 2), Zi(p, 1))
	z = NewPoly().Sub(x, y)
	assert.Equal(t, 1, z.degree)
	assert.Equal(t, int64(1), z.coeff[0].Int64())
	assert.Equal(t, int64(1), z.coeff[1].Int64())
	// (x+1) - (x^2+2x+1) = (-x^2 - x)
	x = NewPoly(Zi(p, 1), Zi(p, 1))
	y = NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	z = NewPoly().Sub(x, y)
	assert.Equal(t, 2, z.degree)
	assert.Equal(t, int64(0), z.coeff[0].Int64())
	assert.Equal(t, Zi(p, -1).Int64(), z.coeff[1].Int64())
	assert.Equal(t, Zi(p, -1).Int64(), z.coeff[2].Int64())
}

func TestPolyDivmod(t *testing.T) {
	// (x^2 + 2x + 1) / (x + 1) = (x + 1)
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	y := NewPoly(Zi(p, 1), Zi(p, 1))
	q, r, err := PolyDivmod(x, y)
	t.Logf("q=(%v) r=(%v) err=(%v)", q, r, err)
	assert.Equal(t, 1, q.degree)
	assert.Equal(t, int64(1), q.coeff[0].Int64())
	assert.Equal(t, int64(1), q.coeff[1].Int64())
	assert.Equal(t, 2, len(q.coeff))
	assert.Equalf(t, err, nil, "%v", err)
	assert.Equal(t, 0, r.degree)
	assert.Equal(t, nil, err)
}

func TestGcd(t *testing.T) {
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	y := NewPoly(Zi(p, 1), Zi(p, 1))
	r, err := PolyGcd(x, y)
	assert.Equal(t, nil, err)
	t.Logf("r=(%v)", r)
	assert.Equal(t, 1, r.degree)
	assert.Equal(t, int64(1), r.coeff[0].Int64())
	assert.Equal(t, int64(1), r.coeff[1].Int64())
	assert.Equal(t, 2, len(r.coeff))
}
