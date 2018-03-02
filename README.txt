use 'godoc cmd/github.com/jackdoe/go-rochefort-client' for documentation on the github.com/jackdoe/go-rochefort-client command 

PACKAGE DOCUMENTATION

package rochefort
    import "github.com/jackdoe/go-rochefort-client"

    this package provides a client for https://github.com/jackdoe/rochefort
    disk speed append + offset service (poor man's kafka)

TYPES

type Client struct {
    // contains filtered or unexported fields
}

func NewClient(url string, httpClient *http.Client) *Client
    Creates new client, takes rochefort url and http client (or nil, at
    which case it uses a client with 1 second timeout)

func (this *Client) Append(namespace string, tags []string, allocSize uint32, data []byte) (uint64, error)
    Append to the rochefort service, returns stored offset and error. in
    case of error the returned offset is 0, keep in mind that 0 is valid
    offset, so check the error field allocSize parameter is used if you want
    to allocate more space than your data, so you can inplace modify it; can
    be 0 the tags parameter is used to build online inverted index that can
    be used from Scan()

func (this *Client) Get(namespace string, offset uint64) ([]byte, error)
    Get from rochefort, use the offset returned by Append

func (this *Client) GetMulti(namespace string, offsets []uint64) ([][]byte, error)
    GetMulti fetches multiple records in one round trip

func (this *Client) Modify(namespace string, offset uint64, position uint32, data []byte) (bool, error)

func (this *Client) Scan(namespace string, tags []string, callback func(rochefortOffset uint64, value []byte)) error
    Scan the whole namespace, callback called with rochefortOffset and the
    value at this offset if you pass tags argument (e.g. a,b) it will scan
    only the offsets tagged with the specific tags (e.g. a,b)


