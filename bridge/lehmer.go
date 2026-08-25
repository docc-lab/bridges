package bridge

import (
	"fmt"
	"math/big"
)

// partialPermutationBytes is the fixed number of bytes needed to encode an
// ordered selection of k distinct values from a universe of n values.  The
// number of such selections is P(n,k) = n!/(n-k)!.
func partialPermutationBytes(n, k int) int {
	if n < 0 || k < 0 || k > n {
		return 0
	}
	count := big.NewInt(1)
	factor := new(big.Int)
	for i := 0; i < k; i++ {
		count.Mul(count, factor.SetInt64(int64(n-i)))
	}
	// Ranks occupy [0,count). A singleton alphabet needs no rank bytes.
	count.Sub(count, big.NewInt(1))
	return (count.BitLen() + 7) / 8
}

// fenwickSet represents the still-available values 1..n. It supports Lehmer
// digit lookup and selection in O(log n), avoiding quadratic slice deletion
// for wide sibling sets.
type fenwickSet []int

func newFenwickSet(n int) fenwickSet {
	f := make(fenwickSet, n+1)
	for i := 1; i <= n; i++ {
		f[i] = i & -i
	}
	return f
}

func (f fenwickSet) prefix(i int) int {
	n := 0
	for i > 0 {
		n += f[i]
		i -= i & -i
	}
	return n
}

func (f fenwickSet) remove(i int) {
	for j := i; j < len(f); j += j & -j {
		f[j]--
	}
}

// selectRank returns the value whose zero-based rank among the remaining
// values is rank.
func (f fenwickSet) selectRank(rank int) int {
	idx := 0
	bit := 1
	for bit<<1 < len(f) {
		bit <<= 1
	}
	for bit > 0 {
		next := idx + bit
		if next < len(f) && f[next] <= rank {
			idx = next
			rank -= f[next]
		}
		bit >>= 1
	}
	return idx + 1
}

// encodePartialPermutation Lehmer-encodes seq, an ordered selection of k
// distinct one-based values from 1..n. The result is a fixed-width, big-endian
// rank, so the caller need only encode n/k (or otherwise derive them).
func encodePartialPermutation(n int, seq []int) ([]byte, error) {
	k := len(seq)
	if n < 0 || k > n {
		return nil, fmt.Errorf("invalid partial permutation dimensions n=%d k=%d", n, k)
	}
	f := newFenwickSet(n)
	rank := new(big.Int)
	base := new(big.Int)
	for i, v := range seq {
		if v < 1 || v > n || f.prefix(v)-f.prefix(v-1) == 0 {
			return nil, fmt.Errorf("value %d at index %d is outside 1..%d or repeated", v, i, n)
		}
		digit := f.prefix(v - 1)
		rank.Mul(rank, base.SetInt64(int64(n-i)))
		rank.Add(rank, base.SetInt64(int64(digit)))
		f.remove(v)
	}
	w := partialPermutationBytes(n, k)
	out := make([]byte, w)
	rank.FillBytes(out)
	return out, nil
}

// decodePartialPermutation reverses encodePartialPermutation. Exactly the
// fixed-width bytes implied by n and k must be supplied.
func decodePartialPermutation(n, k int, encoded []byte) ([]int, error) {
	w := partialPermutationBytes(n, k)
	if n < 0 || k < 0 || k > n || len(encoded) != w {
		return nil, fmt.Errorf("invalid partial permutation n=%d k=%d bytes=%d (want %d)", n, k, len(encoded), w)
	}
	if k == 0 {
		return nil, nil
	}
	rank := new(big.Int).SetBytes(encoded)
	digits := make([]int, k)
	q, rem, base := new(big.Int), new(big.Int), new(big.Int)
	for i := k - 1; i >= 0; i-- {
		q.QuoRem(rank, base.SetInt64(int64(n-i)), rem)
		digits[i] = int(rem.Int64())
		rank.Set(q)
	}
	if rank.Sign() != 0 {
		return nil, fmt.Errorf("Lehmer rank is outside P(%d,%d)", n, k)
	}
	f := newFenwickSet(n)
	out := make([]int, k)
	for i, digit := range digits {
		remaining := n - i
		if digit < 0 || digit >= remaining {
			return nil, fmt.Errorf("invalid Lehmer digit %d at index %d", digit, i)
		}
		out[i] = f.selectRank(digit)
		f.remove(out[i])
	}
	return out, nil
}
