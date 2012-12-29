package conflux

import (
	"errors"
	"math/big"
)

var InterpolationFailure = errors.New("Interpolation failed")

func abs(x int) int {
	if x < 0 {
		return 0 - x
	}
	return x
}

func Interpolate(values []*Zp, points []*Zp, degDiff int) (rfn *RationalFn, err error) {
	if abs(degDiff) > len(values) {
		err = InterpolationFailure
		return
	}
	p := values[0].P
	mbar := len(values)
	if (len(values) + degDiff) % 2 != 0 {
		mbar = len(values) - 1
	} else {
		mbar = len(values)
	}
	ma := (mbar + degDiff) / 2
	mb := (mbar - degDiff) / 2
	matrix := NewMatrix(mbar, mbar+1, &Zp{ Int: big.NewInt(int64(0)), P: p })
	for j := 0; j < mbar; j++ {
		accum := Zi(p, 1)
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
	acoeffs[ma] = Zi(p, 1)
	for j := 0; j < ma; j++ {
		acoeffs[j] = matrix.Get(mbar, j)
	}
	apoly := NewPoly(acoeffs...)
	// Fill 'B' coefficients
	bcoeffs := make([]*Zp, mb+1)
	acoeffs[mb] = Zi(p, 1)
	for j := 0; j < mb; j++ {
		acoeffs[j] = matrix.Get(mbar, j + ma)
	}
	bpoly := NewPoly(bcoeffs...)
	// Reduce
	g, err := PolyGcd(apoly, bpoly)
	if err != nil {
		return nil, err
	}
	rfn = &RationalFn{}
	rfn.Num, err = PolyDiv(apoly, g)
	if err != nil {
		return nil, err
	}
	rfn.Denom, err = PolyDiv(bpoly, g)
	return
}

var LowMBar error = errors.New("Low MBar")

// PolyPowmod calculates a^n mod b where a and b are
// polynomials, n is an integer Z(P).
func PolyPowmod(a, b *Poly, n *Zp) *Poly {
	var err error
	nbits := n.BitLen()
	rval := NewPoly(Zi(n.P, 1))
	x2n := a
	for bit := 0; bit < nbits; bit++ {
		if n.Bit(bit) != 0 {
			rval.Mul(rval, x2n).Mul(rval, b)
		}
		x2n.Mul(x2n, x2n)
		x2n, err = PolyMod(x2n, b)
		if err != nil {
			panic(err)
		}
	}
	return rval
}

/*
let powmod ~modulus x n =             
  let nbits = Number.nbits n in       
  let rval = ref Poly.one in          
  let x2n = ref x in                  
  for bit = 0 to nbits do             
    if Number.nth_bit n bit then      
      rval := mult modulus !rval !x2n;
    x2n := square modulus !x2n        
  done;                               
  !rval                               
*/

func (p *Poly) genSplitter() *Poly {
	panic("TODO")
}

/*
let gen_splitter f =
  let q =  ZZp.neg ZZp.one /: ZZp.two in
  let a =  rand_ZZp () in
  let za = Poly.of_array [| a ; ZZp.one |] in
  let zaq = powmod ~modulus:f za (ZZp.to_number q) in
  let zaqo = Poly.sub zaq Poly.one in
  zaqo
	
*/

func (p *Poly) RandSplit() (first, second *Poly) {
	var err error
	splitter := p.genSplitter()
	first, err = PolyGcd(splitter, p)
	if err != nil {
		panic(err)
	}
	second, err = PolyDiv(p, first)
	if err != nil {
		panic(err)
	}
	return
}

/*
let rec rand_split f =
  let splitter = gen_splitter f in
  let first = Poly.gcd splitter f in
  let second = Poly.div f first in
  (first,second)
*/

func (p *Poly) Factor() (result *ZSet) {
	result = &ZSet{}
	if p.degree == 1 {
		constCoeff := p.coeff[0]
		result.Add(constCoeff.Copy().Neg())
	} else if p.degree > 1 {
		p1, p2 := p.RandSplit()
		result.AddAll(p1.Factor())
		result.AddAll(p2.Factor())
	}
	return
}

func factorCheck(p *Poly) bool {
	panic("TODO")
}

func Reconcile(values []*Zp, points []*Zp, degDiff int) (*ZSet, *ZSet, error) {
	rfn, err := Interpolate(
			values[:len(values)-1], points[:len(points)-1], degDiff)
	if err != nil {
		return nil, nil, err
	}
	lastPoint := points[len(points)-1]
	valFromPoly := Z(lastPoint.P).Div(
			rfn.Num.Eval(lastPoint), rfn.Denom.Eval(lastPoint))
	lastValue := values[len(values)-1]
	if valFromPoly.Cmp(lastValue) != 0 ||
			!factorCheck(rfn.Num) || !factorCheck(rfn.Denom) {
		return nil, nil, LowMBar
	}
	return rfn.Num.Factor(), rfn.Denom.Factor(), nil
}
