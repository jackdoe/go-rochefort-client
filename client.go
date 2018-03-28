/*
this package provides a client for https://github.com/jackdoe/rochefort disk speed append + offset service (poor man's kafka)
*/
package rochefort

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	url        string
	getUrl     string
	setUrl     string
	queryUrl   string
	scanUrl    string
	deleteUrl  string
	compactUrl string
	http       *http.Client
}

// Creates new client, takes rochefort url and http client (or nil, at which case it uses a client with 1 second timeout)
func NewClient(url string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 64,
			},
			Timeout: time.Duration(1) * time.Second,
		}
	}
	if !strings.HasSuffix(url, "/") {
		url = fmt.Sprintf("%s/", url)
	}

	return &Client{
		url:        url,
		getUrl:     fmt.Sprintf("%sget", url),
		setUrl:     fmt.Sprintf("%sset", url),
		queryUrl:   fmt.Sprintf("%squery", url),
		scanUrl:    fmt.Sprintf("%sscan", url),
		compactUrl: fmt.Sprintf("%scompact", url),
		deleteUrl:  fmt.Sprintf("%sdelete", url),
		http:       httpClient,
	}
}

// Set to the rochefort service, returns stored offset and error. in case of error the returned offset is 0, keep in mind that 0 is valid offset, so check the error field
// allocSize parameter is used if you want to allocate more space than your data, so you can inplace modify it; can be 0
// the tags parameter is used to build online inverted index that can be used from Scan()
func (this *Client) Set(input *AppendInput) (*AppendOutput, error) {
	data, err := input.Marshal()
	if err != nil {
		return nil, err
	}
	resp, err := this.http.Post(this.setUrl, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nonOkError(resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := &AppendOutput{}
	err = out.Unmarshal(body)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (this *Client) Compact(input *NamespaceInput) (*SuccessOutput, error) {
	data, err := input.Marshal()
	if err != nil {
		return nil, err
	}
	resp, err := this.http.Post(this.compactUrl, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nonOkError(resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := &SuccessOutput{}
	err = out.Unmarshal(body)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (this *Client) Delete(input *NamespaceInput) (*SuccessOutput, error) {
	data, err := input.Marshal()
	if err != nil {
		return nil, err
	}
	resp, err := this.http.Post(this.deleteUrl, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nonOkError(resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := &SuccessOutput{}
	err = out.Unmarshal(body)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func nonOkError(code int, body io.Reader) error {
	b, err := ioutil.ReadAll(body)
	if err != nil {
		return errors.New(fmt.Sprintf("expected status code 200, but got: %d, couldnt read the body got: %s", code, err.Error()))
	} else {
		return errors.New(fmt.Sprintf("expected status code 200, but got: %d, body: %s", code, string(b)))
	}
}

// Get fetches multiple records in one round trip
func (this *Client) Get(input *GetInput) ([][]byte, error) {
	b, err := input.Marshal()
	if err != nil {
		return nil, err
	}

	resp, err := this.http.Post(this.getUrl, "application/octet-stream", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nonOkError(resp.StatusCode, resp.Body)
	}
	// XXX: read stream
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := &GetOutput{}
	err = out.Unmarshal(body)
	if err != nil {
		return nil, err
	}
	return out.Data, nil
}

// Scan the whole namespace, callback called with rochefortOffset and the value at this offset
func (this *Client) Scan(namespace string, callback func(rochefortOffset uint64, value []byte)) error {
	url := fmt.Sprintf("%s?namespace=%s", this.scanUrl, namespace)

	resp, err := this.http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nonOkError(resp.StatusCode, resp.Body)
	}

	header := make([]byte, 12)

	for {
		_, err := io.ReadFull(resp.Body, header)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		len := binary.LittleEndian.Uint32(header)
		offset := binary.LittleEndian.Uint64(header[4:])

		data := make([]byte, len)
		_, err = io.ReadFull(resp.Body, data)
		if err != nil {
			return errors.New(fmt.Sprintf("expected at least %d bytes, but got EOF, error: %s", len, err.Error()))
		}

		callback(offset, data)
	}
	return nil
}

// Search the whole namespace based on the tagged (with Append tags) blobs, callback called with rochefortOffset and the value at this offset
// example:
//
// r.Search(ns, map[string]interface{}{
// 	"or": []interface{}{
// 		map[string]interface{}{
// 			"tag": "a",
// 		},
// 		map[string]interface{}{
// 			"tag": "b",
// 		},
// 	},
// }, func(offset uint64, data []byte) {
// 	scanned = append(scanned, string(data))
// })
func (this *Client) Search(namespace string, query map[string]interface{}, callback func(rochefortOffset uint64, value []byte)) error {
	url := fmt.Sprintf("%s?namespace=%s", this.queryUrl, namespace)

	j, err := json.Marshal(query)
	if err != nil {
		return err
	}

	resp, err := this.http.Post(url, "application/json", bytes.NewBuffer(j))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nonOkError(resp.StatusCode, resp.Body)
	}

	header := make([]byte, 12)

	for {
		_, err := io.ReadFull(resp.Body, header)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		len := binary.LittleEndian.Uint32(header)
		offset := binary.LittleEndian.Uint64(header[4:])

		data := make([]byte, len)
		_, err = io.ReadFull(resp.Body, data)
		if err != nil {
			return errors.New(fmt.Sprintf("expected at least %d bytes, but got EOF, error: %s", len, err.Error()))
		}

		callback(offset, data)
	}
	return nil
}
