package rochefort

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}
func generateTestCases(n int) [][]byte {
	out := make([][]byte, n)
	for i := 1; i <= n; i++ {
		out = append(out, randBytes(i))
	}
	return out
}

func TestEverything(t *testing.T) {
	host := os.Getenv("ROCHEFORT_TEST")
	if host == "" {
		t.Skip("skipping test because of no ROCHEFORT_HOST env")
	}
	r := NewClient(host, nil)

	for _, ns := range []string{"", "ns1", "ns2", "ns3"} {
		cases := generateTestCases(100)
		offsets := make([]uint64, 0)
		added := make([][]byte, 0)
		indexed := make(map[uint64][]byte)

		err := r.Scan(ns, func(offset uint64, data []byte) {
			indexed[offset] = data
		})
		t.Logf("ns: %s, initial scan: %d", ns, len(indexed))

		if err != nil {
			t.Log(err)
			t.FailNow()
		}

		for _, currentcase := range cases {
			off, err := r.Append(ns, string(randBytes(30)), currentcase)
			if err != nil {
				t.Log(err)
				t.FailNow()
			}
			offsets = append(offsets, off)

			fetched, err := r.Get(ns, off)
			if err != nil {
				t.Log(err)
				t.FailNow()
			}
			if !bytes.Equal(fetched, currentcase) {
				t.Log("fetched != case")
				t.FailNow()
			}

			indexed[off] = currentcase

			added = append(added, fetched)
			many, err := r.GetMulti(ns, offsets)
			if err != nil {
				t.Log(err)
				t.FailNow()
			}

			for i, v := range many {
				if !bytes.Equal(added[i], v) {
					t.Log("fetched != case")
					t.FailNow()
				}
			}

			err = r.Scan(ns, func(offset uint64, data []byte) {
				v, ok := indexed[offset]
				if !ok {
					t.Log("missing offset from scan")
					t.FailNow()
				}
				if !bytes.Equal(v, data) {
					t.Log("fetched != case")
					t.FailNow()
				}
			})
			if err != nil {
				t.Log(err)
				t.FailNow()
			}

		}
	}
}

func BenchmarkSetAndGet(b *testing.B) {
	host := os.Getenv("ROCHEFORT_TEST")
	if host == "" {
		panic("skipping test because of no ROCHEFORT_HOST env")
	}
	r := NewClient(host, nil)
	key := string(randBytes(10))
	value := randBytes(30)
	for n := 0; n < b.N; n++ {
		off, err := r.Append("", key, value)
		if err != nil {
			panic(err)
		}
		_, err = r.Get("", off)
		if err != nil {
			panic(err)
		}
	}
}
