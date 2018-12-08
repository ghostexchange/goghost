package es

type Connector interface {
	Search(target *SearchResult, indict, searchBody string) error
	ScrollSearch(target *[]SearchResult, indict, scroll, searchBody string) error
	Update(indict, id, updateBody string) error
	Save(indict, id, saveBody string) error
	Delete(indict, deleteBody string) error
}

type _Pool struct {
	pool map[string]Connector
}

var pool *_Pool

func init() {
	if pool != nil {
		return
	}
	pool = &_Pool{make(map[string]Connector)}
}

func GetConnector(name string) Connector {
	return pool.pool[name]
}
