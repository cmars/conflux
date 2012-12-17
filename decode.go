package conflux

import (
	"errors"
)

var InterpolationFailure = errors.New("Interpolation failed")

func Interpolate(values []*Zp, points []*Zp, degDiff int) (rfn *RationalFn, err error) {
	if math.Abs(degDiff) > len(values) {
		err = InterpolationFailure
		return
	}
	p := values[0].P
	mbar := len(values)
	if (len(values) + d) % 2 != 0 {
		mbar = len(values) - 1
	} else {
		mbar = len(values)
	}
	ma := (mbar + d) / 2
	mb := (mbar - d) / 2
	// TODO: implement matrix
	matrix := NewMatrix(mbar, mbar+1, &Zp{ Int: big.NewInt(int64(0)), P: p })
	for j := 0; j < mbar; j++ {
		accum := NewZp(p, 1)
		kj := points[j]
		fj := values[j]
		for i := 0; i < ma; i++ {
			matrix.Set(i, j, accum)
			accum.Mul(accum, kj)
		}
		kjma := accum.Copy()
		accum = fj.Copy().Neg()
		for i := ma; i < mbar; i++ {
			matrix.Set(i, j, accum)
			accum.Mul(accum, kj)
		}
		fjkjmb := accum.Copy().Neg()
		matrix.Set(mbar, j, fjkjmb.Copy().Sub(fjkjmb, kjma))
	}
	err = matrix.Reduce()
	if err != nil {
		return
	}
	// Fill 'A' coefficients
	acoeffs := make([]*Zp, ma+1)
	acoeffs[ma] = NewZp(p, 1)
	for j := 0; j < ma; j++ {
		acoeffs[j] = matrix.Get(mbar, j)
	}
	apoly := NewPoly(acoeffs)
	// Fill 'B' coefficients
	bcoeffs := make([]*Zp, mb+1)
	acoeffs[mb] = NewZp(p, 1)
	for j := 0; j < mb; j++ {
		acoeffs[j] = matrix.Get(mbar, j + ma)
	}
	bpoly := NewPoly(bcoeffs)
	// Reduce
	g := PolyGcd(apoly, bpoly)
	rfn = &RationalFn{ Num: apoly.Div(g), Denom: bpoly.Div(g) }
	return
}
