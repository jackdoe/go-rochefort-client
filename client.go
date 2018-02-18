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
	"net/url"
	"strings"
	"time"
)

type Client struct {
	url         string
	getUrl      string
	getMultiUrl string
	appendUrl   string
	scanUrl     string
	http        *http.Client
}

type appendResponse struct {
	Offset uint64
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
		url:         url,
		getUrl:      fmt.Sprintf("%sget", url),
		getMultiUrl: fmt.Sprintf("%sgetMulti", url),
		appendUrl:   fmt.Sprintf("%sappend", url),
		scanUrl:     fmt.Sprintf("%sscan", url),
		http:        httpClient,
	}
}

// Append to the rochefort service, returns stored offset and error. in case of error the returned offset is 0, keep in mind that 0 is valid offset, so check the error field
func (this *Client) Append(namespace, id string, data []byte) (uint64, error) {
	url := fmt.Sprintf("%s?id=%s&namespace=%s", this.appendUrl, url.PathEscape(id), namespace)
	resp, err := this.http.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, nonOkError(resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var offsetResponse appendResponse
	if err := json.Unmarshal(body, &offsetResponse); err != nil {
		return 0, err
	}
	return offsetResponse.Offset, nil
}

func nonOkError(code int, body io.Reader) error {
	b, err := ioutil.ReadAll(body)
	if err != nil {
		return errors.New(fmt.Sprintf("expected status code 200, but got: %d, couldnt read the body got: %s", code, err.Error()))
	} else {
		return errors.New(fmt.Sprintf("expected status code 200, but got: %d, body: %s", code, string(b)))
	}

}

// Get from rochefort, use the offset returned by Append
func (this *Client) Get(namespace string, offset uint64) ([]byte, error) {
	url := fmt.Sprintf("%s?offset=%d&namespace=%s", this.getUrl, offset, namespace)
	resp, err := this.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nonOkError(resp.StatusCode, resp.Body)
	}

	return ioutil.ReadAll(resp.Body)
}

// GetMulti fetches multiple records in one round trip
func (this *Client) GetMulti(namespace string, offsets []uint64) ([][]byte, error) {
	url := fmt.Sprintf("%s?namespace=%s", this.getMultiUrl, namespace)

	encodedOffsets := make([]byte, len(offsets)*8)
	for i, offset := range offsets {
		binary.LittleEndian.PutUint64(encodedOffsets[(i*8):], offset)
	}

	resp, err := this.http.Post(url, "application/octet-stream", bytes.NewReader(encodedOffsets))
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
	offset := uint32(0)
	out := make([][]byte, 0)
	bodyLen := uint32(len(body))
	for offset < bodyLen {
		len := binary.LittleEndian.Uint32(body[offset:])
		out = append(out, body[(offset+4):(offset+4+len)])
		offset += 4 + len
	}
	return out, nil
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
			return errors.New(fmt.Sprintf("expected at least %d bytes, but got EOF, error: %s", len, err.Error))
		}

		callback(offset, data)
	}
	return nil
}
