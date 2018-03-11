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
	offsets, err := r.Set(&AppendInput{
		AppendPayload: []*Append{{
			Namespace: ns,
			AllocSize: 5,
			Data:      []byte("abc"),
		}},
	})
	off := offsets.Offset[0]
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	_, err = r.Set(&AppendInput{
		ModifyPayload: []*Modify{{
			Namespace: ns,
			Offset:    off,
			Pos:       1,
			Data:      []byte("zxcv"),
		}},
	})

	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	data, err := r.Get(&GetInput{
		GetPayload: []*Get{{
			Namespace: ns,
			Offset:    off,
		}},
	})
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	if string(data[0]) != "azxcv" {
		t.Logf("unexpected read: %s", string(data[0]))
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
	_, err := r.Set(&AppendInput{
		AppendPayload: []*Append{{
			Namespace: ns,
			Tags:      []string{"a"},
			AllocSize: 0,
			Data:      []byte("aaa"),
		}},
	})

	if err != nil {
		panic(err)
	}

	_, err = r.Set(&AppendInput{
		AppendPayload: []*Append{{
			Namespace: ns,
			Tags:      []string{"b"},
			AllocSize: 0,
			Data:      []byte("bbb"),
		}},
	})
	if err != nil {
		panic(err)
	}
	scanned := []string{}
	r.Search(ns, map[string]interface{}{
		"or": []interface{}{
			map[string]interface{}{
				"tag": "a",
			},
			map[string]interface{}{
				"tag": "b",
			},
		},
	}, func(offset uint64, data []byte) {
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

		err := r.Scan(ns, func(offset uint64, data []byte) {
			indexed[offset] = data
		})
		t.Logf("ns: %s, initial scan: %d", ns, len(indexed))

		if err != nil {
			t.Log(err)
			t.FailNow()
		}

		for _, currentcase := range cases {
			off, err := r.Set(&AppendInput{
				AppendPayload: []*Append{{
					Namespace: ns,
					Data:      currentcase,
				}, {
					Namespace: ns,
					Data:      []byte("zxc"),
				}},
			})
			if err != nil {
				t.Log(err)
				t.FailNow()
			}
			offsets = append(offsets, off.Offset[0], off.Offset[1])

			fetched, err := r.Get(&GetInput{
				GetPayload: []*Get{{
					Namespace: ns,
					Offset:    off.Offset[0],
				}, {
					Namespace: ns,
					Offset:    off.Offset[1],
				}},
			})

			if err != nil {
				t.Log(err)
				t.FailNow()
			}
			if !bytes.Equal(fetched[0], currentcase) {
				t.Log("fetched != case")
				t.FailNow()
			}

			indexed[off.Offset[0]] = currentcase
			indexed[off.Offset[1]] = []byte("zxc")

			added = append(added, fetched[0], fetched[1])

			multiRequest := &GetInput{
				GetPayload: []*Get{},
			}
			for _, offset := range offsets {
				multiRequest.GetPayload = append(multiRequest.GetPayload, &Get{
					Namespace: ns,
					Offset:    offset,
				})
			}
			many, err := r.Get(multiRequest)
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
		panic("skipping test because of no ROCHEFORT_TEST env")
	}
	r := NewClient(host, nil)
	value := randBytes(30)
	for n := 0; n < b.N; n++ {
		off, err := r.Set(&AppendInput{
			AppendPayload: []*Append{{
				Namespace: "bench",
				Data:      value,
			}},
		})
		if err != nil {
			panic(err)
		}
		_, err = r.Get(&GetInput{
			GetPayload: []*Get{{
				Namespace: "bench",
				Offset:    off.Offset[0],
			}},
		})
		if err != nil {
			panic(err)
		}
	}
}
