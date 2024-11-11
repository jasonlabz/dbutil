package element

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
)

var (
	benchDecimal = &Decimal{
		value: decimal.New(math.MaxInt64, -9),
	}

	benchDecimalStr = &DecimalStr{
		value:  strconv.FormatInt(math.MaxInt64, 10),
		intLen: 9,
	}

	benchFloat64 = &Float64{
		value: float64(math.MaxInt64) / 1e9,
	}

	benchInt64 = &Int64{
		value: math.MaxInt64,
	}

	benchBigInt = &BigInt{
		value: big.NewInt(math.MaxInt64),
	}

	benchBigIntStr = &BigIntStr{
		value: strconv.FormatInt(math.MaxInt64, 10),
	}
)

func BenchmarkConverter_ConvertFromBigInt(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = testNumConverter.ConvertBigIntFromInt(in[i])
	}
}

func BenchmarkOldConverter_ConvertFromBigInt(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = testOldNumConverter.ConvertBigIntFromInt(in[i])
	}
}

func BenchmarkConverter_ConvertDecimalFromloat(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]float64, b.N)
	for i := range in {
		in[i] = rng.NormFloat64() * 10e20
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = testNumConverter.ConvertDecimalFromFloat(in[i])
	}
}

func BenchmarkOldConverter_ConvertDecimalFromFloat(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]float64, b.N)
	for i := range in {
		in[i] = rng.NormFloat64() * 10e20
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = testOldNumConverter.ConvertDecimalFromFloat(in[i])
	}
}

func BenchmarkConverter_ConvertBigInt_Int64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatInt(in[i], 10)
		_, _ = testNumConverter.ConvertBigInt(in)
	}
}

func BenchmarkOldConverter_ConvertBigInt_Int64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatInt(in[i], 10)
		_, _ = testOldNumConverter.ConvertBigInt(in)
	}
}

func BenchmarkCoventor_ConvertBigInt_large_number(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "93233720368547758079223372036854775807")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			_, _ = testNumConverter.ConvertBigInt(p)
		}
	}
}

func BenchmarkOldCoventor_ConvertBigInt_large_number(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "93233720368547758079223372036854775807")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			_, _ = testOldNumConverter.ConvertBigInt(p)
		}
	}
}

func BenchmarkConverter_ConvertDecimal_Int64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatInt(in[i], 10)
		_, _ = testNumConverter.ConvertDecimal(in)
	}
}

func BenchmarkOldConverter_ConvertDecimal_Int64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]int64, b.N)
	for i := range in {
		in[i] = int64(rng.Intn(math.MaxInt64))
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatInt(in[i], 10)
		_, _ = testOldNumConverter.ConvertDecimal(in)
	}
}

func BenchmarkConverter_ConvertDecimal_Float64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]float64, b.N)
	for i := range in {
		in[i] = rng.NormFloat64() * 10e20
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatFloat(in[i], 'f', -1, 64)
		_, _ = testNumConverter.ConvertDecimal(in)
	}
}

func BenchmarkOldConverter_ConvertDecimal_Float64(b *testing.B) {
	rng := rand.New(rand.NewSource(0xdead1337))
	in := make([]float64, b.N)
	for i := range in {
		in[i] = rng.NormFloat64() * 10e20
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in := strconv.FormatFloat(in[i], 'f', -1, 64)
		_, _ = testOldNumConverter.ConvertDecimal(in)
	}
}

func BenchmarkConverter_ConvertDecimal(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, fmt.Sprintf("%d.%d", i*100, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkOldConverter_ConvertDecimal(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, fmt.Sprintf("%d.%d", i*100, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testOldNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkConverter_ConvertDecimal_large_number(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "9323372036854775807.9223372036854775807")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkOldConverter_ConvertDecimal_large_number(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "9323372036854775807.9223372036854775807")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testOldNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkConverter_ConvertDecimal_Exp(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "9323372036854775807.922e1234567890")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkOldConverter_ConvertDecimal_Exp(b *testing.B) {
	count := 72
	prices := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		prices = append(prices, "9323372036854775807.922e1234567890")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range prices {
			d, err := testOldNumConverter.ConvertDecimal(p)
			if err != nil {
				b.Log(d)
				b.Error(err)
			}
		}
	}
}

func BenchmarkDecimal_Decmial_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchDecimal.String()
	}
}

func BenchmarkDecimal_DecmialStr_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchDecimalStr.String()
	}
}

func BenchmarkDecimal_Float64_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchFloat64.String()
	}
}

func BenchmarkDecimal_Int64_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchInt64.String()
	}
}

func BenchmarkDecimal_BigInt_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchBigInt.String()
	}
}

func BenchmarkDecimal_BigIntStr_String(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchBigIntStr.String()
	}
}
