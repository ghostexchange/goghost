package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type ES struct {
	host     string
	username string
	password string
}

func AddES(name, host, username, password string) *ES {
	conn := &ES{
		host,
		username,
		password,
	}
	pool.pool[name] = conn
	return conn
}

type SearchResult struct {
	Status       int              `json:"status"`
	Hits         *SearchHits      `json:"hits"`
	Aggregations *json.RawMessage `json:"aggregations"`
	Error        *json.RawMessage `json:"error"`
	ScrollId     *json.RawMessage `json:"_scroll_id"`
}

type SearchHits struct {
	Count int64            `json:"total"`
	Hits  *json.RawMessage `json:"hits"`
}

type AggregationsResult struct {
	Key     string                `json:"key"`
	Count   int64                 `json:"doc_count"`
	Other   int64                 `json:"sum_other_doc_count"`
	Buckets []*AggregationsResult `json:"buckets"`
}

func Bind(target interface{}, raw *json.RawMessage) error {
	data, _ := raw.MarshalJSON()
	if err := json.Unmarshal(data, target); err != nil {
		return err
	}
	return nil
}

func (this *ES) Search(target *SearchResult, indict, searchBody string) error {
	log.Debug(
		http.MethodGet,
		this.host+"/"+indict+"/_search",
		searchBody,
	)
	request, err := http.NewRequest(
		http.MethodGet,
		this.host+"/"+indict+"/_search",
		bytes.NewReader([]byte(searchBody)),
	)
	request.Header.Add("Content-Type", "application/json")
	request.SetBasicAuth(
		this.username,
		this.password,
	)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := json.Unmarshal(body, target); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}

	if target.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}

	return nil
}

func (this *ES) ScrollSearch(targets *[]SearchResult, indict, scroll, searchBody string) error {
	log.Debug(
		http.MethodGet,
		this.host+"/"+indict+"/_search?scroll="+scroll,
		searchBody,
	)
	request, err := http.NewRequest(
		http.MethodGet,
		this.host+"/"+indict+"/_search?scroll="+scroll,
		bytes.NewReader([]byte(searchBody)),
	)
	request.Header.Add("Content-Type", "application/json")
	request.SetBasicAuth(
		this.username,
		this.password,
	)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	first := SearchResult{}
	if err := json.Unmarshal(body, &first); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}
	if first.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}

	*targets = append(*targets, first)
	for {
		scrollId := string(*first.ScrollId)
		scrollId = scrollId[1 : len(scrollId)-1]
		target := SearchResult{}
		if err := this.scroll(&target, scroll, scrollId); err != nil {
			return err
		}
		if target.Error != nil {
			log.Error(err, string(*target.Error))
			return fmt.Errorf("%s\n", string(*target.Error))
		}
		if string(*target.Hits.Hits) == "[]" {
			break
		}
		*targets = append(*targets, target)
	}

	return nil
}

func (this *ES) scroll(target *SearchResult, scroll, scrollId string) error {

	searchBody := fmt.Sprintf(`{"scroll":"%s","scroll_id":"%s"}`, scroll, scrollId)
	log.Debug(
		http.MethodGet,
		this.host+"/_search/scroll",
		searchBody,
	)
	request, err := http.NewRequest(
		http.MethodGet,
		this.host+"/_search/scroll",
		bytes.NewReader([]byte(searchBody)),
	)
	request.Header.Add("Content-Type", "application/json")
	request.SetBasicAuth(
		this.username,
		this.password,
	)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := json.Unmarshal(body, target); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}

	if target.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}

	return nil
}

func (this *ES) Save(indict, id, saveBody string) error {
	log.Debug(
		http.MethodPost,
		this.host+"/"+indict+"/_doc/"+id,
		saveBody,
	)

	// if id is empty string the es will generate one
	request, err := http.NewRequest(
		http.MethodPost,
		this.host+"/"+indict+"/_doc/"+id,
		bytes.NewReader([]byte(saveBody)),
	)
	request.Header.Add("Content-Type", "application/json")
	request.SetBasicAuth(
		this.username,
		this.password,
	)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result := struct {
		Error *json.RawMessage `json:"error"`
	}{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}

	if result.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}

	return nil
}

func (this *ES) Update(indict, id, updateBody string) error {
	request, err := http.NewRequest(
		http.MethodPost,
		this.host+"/"+indict+"/_doc/"+id+"/_update",
		bytes.NewReader([]byte(updateBody)),
	)
	body, err := this.sendRequest(request)
	if err != nil {
		return err
	}

	result := struct {
		Error *json.RawMessage `json:"error"`
	}{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}

	if result.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}
	return nil
}

func (this *ES) Delete(indict, deleteBody string) error {
	request, err := http.NewRequest(
		http.MethodPost,
		this.host+"/"+indict+"/_delete_by_query",
		bytes.NewReader([]byte(deleteBody)),
	)
	body, err := this.sendRequest(request)
	if err != nil {
		return err
	}
	result := struct {
		Error *json.RawMessage `json:"error"`
	}{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%v\n%s", err, string(body))
	}
	if result.Error != nil {
		log.Error(err, string(body))
		return fmt.Errorf("%s\n", string(body))
	}
	return nil
}

func (this *ES) sendRequest(request *http.Request) ([]byte, error) {
	request.Header.Add("Content-Type", "application/json")
	request.SetBasicAuth(
		this.username,
		this.password,
	)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return []byte(""), err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte(""), err
	}
	defer resp.Body.Close()

	return body, nil
}
