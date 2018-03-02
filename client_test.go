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

func TestModify(t *testing.T) {
	host := os.Getenv("ROCHEFORT_TEST")
	if host == "" {
		t.Skip("skipping test because of no ROCHEFORT_TEST env")
	}
	r := NewClient(host, nil)
	ns := "modify"
	off, err := r.Append(ns, nil, 5, []byte("abc"))
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	_, err = r.Modify(ns, off, 1, []byte("zxcv"))
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	data, err := r.Get(ns, off)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	if string(data) != "azxcv" {
		t.Logf("unexpected read: %s", string(data))
		t.FailNow()
	}
}

func TestSearch(t *testing.T) {
	host := os.Getenv("ROCHEFORT_TEST")
	if host == "" {
		t.Skip("skipping test because of no ROCHEFORT_TEST env")
	}
	r := NewClient(host, nil)

	ns := "search"
	_, err := r.Append(ns, []string{"a"}, 0, []byte("aaa"))
	if err != nil {
		panic(err)
	}

	_, err = r.Append(ns, []string{"b"}, 0, []byte("bbb"))
	if err != nil {
		panic(err)
	}
	scanned := []string{}
	r.Scan(ns, []string{"a", "b"}, func(offset uint64, data []byte) {
		scanned = append(scanned, string(data))
	})

	if scanned[0] != "aaa" {
		t.Logf("unexpected read: %s", scanned[0])
		t.FailNow()
	}

	if scanned[1] != "bbb" {
		t.Logf("unexpected read: %s", scanned[0])
		t.FailNow()
	}

}

func TestEverything(t *testing.T) {
	host := os.Getenv("ROCHEFORT_TEST")
	if host == "" {
		t.Skip("skipping test because of no ROCHEFORT_TEST env")
	}
	r := NewClient(host, nil)

	for _, ns := range []string{"", "ns1", "ns2", "ns3"} {
		cases := generateTestCases(100)
		offsets := make([]uint64, 0)
		added := make([][]byte, 0)
		indexed := make(map[uint64][]byte)

		err := r.Scan(ns, nil, func(offset uint64, data []byte) {
			indexed[offset] = data
		})
		t.Logf("ns: %s, initial scan: %d", ns, len(indexed))

		if err != nil {
			t.Log(err)
			t.FailNow()
		}

		for _, currentcase := range cases {
			off, err := r.Append(ns, nil, 0, currentcase)
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

			err = r.Scan(ns, nil, func(offset uint64, data []byte) {
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
		panic("skipping test because of no ROCHEFORT_TEST env")
	}
	r := NewClient(host, nil)
	value := randBytes(30)
	for n := 0; n < b.N; n++ {
		off, err := r.Append("", nil, 0, value)
		if err != nil {
			panic(err)
		}
		_, err = r.Get("", off)
		if err != nil {
			panic(err)
		}
	}
}
