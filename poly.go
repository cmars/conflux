package conflux

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
)

// Poly represents a polynomial in a finite field.
type Poly struct {
	// A list of the polynomial coefficients,
	// ordered by ascending degree.
	coeff []*Zp
	// Highest degree of the polynomial
	degree int
	// Finite field P
	p *big.Int
}

// NewPoly creates a polynomial with the given coefficients, in ascending
// degree order. For example, NewPoly(1,-2,3) represents the polynomial
// 3x^2 - 2x + 1.
func NewPoly(coeff... *Zp) *Poly {
	p := &Poly{}
	for i := 0; i < len(coeff); i++ {
		if coeff[i] == nil {
			if p.p == nil {
				continue
			} else {
				coeff[i] = Z(p.p)
			}
		}
		c := coeff[i].Copy().Norm()
		p.coeff = append(p.coeff, c)
		if !c.IsZero() {
			p.degree = i
		}
		// All coefficients must be in same field
		if p.p == nil {
			p.p = c.P
			// Initialize prior nils now that we know P
			for j := 0; j <= i; j++ {
				if p.coeff == nil {
					p.coeff[j] = Z(p.p)
				}
			}
		} else {
			c.assertP(p.p)
		}
	}
	return p
}

func (p *Poly) String() string {
	result := bytes.NewBuffer(nil)
	first := true
	for i := len(p.coeff) - 1; i >= 0; i-- {
		c := p.coeff[i]
		if c.IsZero() {
			continue
		}
		if first {
			first = false
		} else {
			fmt.Fprintf(result, " + ")
		}
		fmt.Fprintf(result, "%v", c.String())
		if i > 0 {
			fmt.Fprintf(result, "z^%d", i)
		}
	}
	return string(result.Bytes())
}

func (p *Poly) Degree() int {
	return p.degree
}

func (p *Poly) Coeff() []*Zp {
	return p.coeff
}

func (p *Poly) P() *big.Int {
	return p.p
}

func (p *Poly) Copy() *Poly {
	newP := &Poly{ degree: p.degree, p: p.p }
	for i := 0; i <= p.degree; i++ {
		newP.coeff = append(newP.coeff, p.coeff[i].Copy())
	}
	return newP
}

func (p *Poly) assertP(fp *big.Int) {
	if p.p.Cmp(fp) != 0 {
		panic(fmt.Sprintf("expected finite field Z(%v), was Z(%v)", fp, p.p))
	}
}

func (p *Poly) Equal(q *Poly) bool {
	p.assertP(q.p)
	if p.degree != q.degree {
		return false
	}
	for i := 0; i <= p.degree; i++ {
		if (p.coeff[i] == nil) != (q.coeff[i] == nil) {
			return false
		}
		if p.coeff[i] != nil && p.coeff[i].Cmp(q.coeff[i]) != 0 {
			return false
		}
	}
	return true
}

func maxInt(x, y int) int {
	if y > x {
		return y
	}
	return x
}

func (p *Poly) Add(x, y *Poly) *Poly {
	x.assertP(y.p)
	p.p = x.p
	p.degree = maxInt(x.degree, y.degree)
	p.coeff = make([]*Zp, p.degree)
	for i := 0; i <= p.degree; i++ {
		p.coeff[i] = Z(x.p)
		if i < x.degree && x.coeff[i] != nil {
			p.coeff[i].Add(p.coeff[i], x.coeff[i])
		}
		if i < y.degree && y.coeff[i] != nil {
			p.coeff[i].Add(p.coeff[i], y.coeff[i])
		}
	}
	return p
}

func (p *Poly) Neg() *Poly {
	for i := 0; i < p.degree; i++ {
		p.coeff[i].Neg()
	}
	return p
}

func (p *Poly) Sub(x, y *Poly) *Poly {
	return p.Add(x, y.Copy().Neg())
}

func (p *Poly) Mul(x, y *Poly) *Poly {
	panic("TODO")
/*
let mult x y = 
  let mdegree = degree x + degree y in
  let prod = { a = Array.make ( mdegree + 1 ) ZZp.zero;
               degree = mdegree ;
             }
  in
  for i = 0 to degree x  do
    for j = 0 to degree y do
      prod.a.(i + j) <- prod.a.(i + j) +: x.a.(i) *: y.a.(j)
    done
  done;
  prod
*/
}

func (p *Poly) IsConstant(c *Zp) bool {
	return p.degree == 0 && p.coeff[0].Cmp(c) == 0
}

func (p *Poly) Eval(z *Zp) *Zp {
	zd := Zi(p.p, 1)
	sum := Zi(p.p, 0)
	for d := 0; d <= p.degree; d++ {
		sum.Add(sum, Z(p.p).Mul(p.coeff[d], zd))
		zd.Mul(zd, z)
	}
	return sum
}

func PolyTerm(degree int, c *Zp) *Poly {
	coeff := make([]*Zp, degree + 1)
	coeff[degree] = c
	return NewPoly(coeff...)
}

func PolyDivmod(x, y *Poly) (q *Poly, r *Poly, err error) {
	x.assertP(y.p)
	if x.IsConstant(Zi(x.p, 0)) {
		return NewPoly(Z(x.p)), NewPoly(Z(y.p)), nil
	} else if y.degree > x.degree {
		return NewPoly(Z(x.p)), x, nil
	}
	degDiff := x.degree - y.degree
	if degDiff < 0 {
		err = errors.New(fmt.Sprintf("Quotient degree %d < dividend %d", x.degree, y.degree))
		return
	}
	c := Z(x.p).Div(x.coeff[x.degree], y.coeff[y.degree])
	m := PolyTerm(degDiff, c)
	newX := NewPoly().Sub(x, NewPoly().Mul(m, y))
	if newX.degree < x.degree || x.degree == 0 {
		// TODO: eliminate recursion
		q, r, err = PolyDivmod(newX, y)
		q.Add(q, m)
	} else {
		err = errors.New("Divmod error")
	}
	return
}

func PolyDiv(x, y *Poly) (q *Poly, err error) {
	q, _, err = PolyDivmod(x, y)
	return
}

func PolyMod(x, y *Poly) (r *Poly, err error) {
	_, r, err = PolyDivmod(x, y)
	return
}

func polyGcd(x, y *Poly) (*Poly, error) {
	if y.IsConstant(Zi(x.p, 0)) {
		return x, nil
	}
	_, r, err := PolyDivmod(x, y)
	if err != nil {
		return nil, err
	}
	return polyGcd(y, r)
}

func PolyGcd(x, y *Poly) (result *Poly, err error) {
	result, err = polyGcd(x, y)
	if err != nil {
		return nil, err
	}
	result.Mul(result, NewPoly(result.coeff[result.degree].Copy().Inv()))
	return result, nil
}

type RationalFn struct {
	Num *Poly
	Denom *Poly
}
