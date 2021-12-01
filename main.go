package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	log "github.com/sirupsen/logrus"
)

type CrawlResult struct {
	Err   error
	Title string
	Url   string
}

type Page interface {
	GetTitle() string
	GetLinks() []string
}

type page struct {
	doc *goquery.Document
}

func NewPage(raw io.Reader) (Page, error) {
	doc, err := goquery.NewDocumentFromReader(raw)
	if err != nil {
		log.WithFields(log.Fields{"func": "NewPage", "doc": doc}).Error(err)
		return nil, err
	}
	return &page{doc: doc}, nil
}

func (p *page) GetTitle() string {
	title := p.doc.Find("title").First().Text()
	log.WithFields(log.Fields{"func": "(page)GetTitle", "title": title}).Info("Got title")
	return p.doc.Find("title").First().Text()
}

func (p *page) GetLinks() []string {
	var urls []string
	p.doc.Find("a").Each(func(_ int, s *goquery.Selection) {
		url, ok := s.Attr("href")
		if ok {
			urls = append(urls, url)
		}
	})

	log.WithFields(log.Fields{"func": "(page)Getlinks", "links": urls}).Info("Got links")
	return urls
}

type Requester interface {
	Get(ctx context.Context, url string) (Page, error)
}

type requester struct {
	timeout time.Duration
}

func NewRequester(timeout time.Duration) requester {
	return requester{timeout: timeout}
}

func (r requester) Get(ctx context.Context, url string) (Page, error) {

	select {
	case <-ctx.Done(): //проверка на закрытие контекста.
		return nil, nil
	default:
		cl := &http.Client{
			Timeout: r.timeout,
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.WithFields(log.Fields{"func": "(requester)Get", "err": err, "url": url}).Errorf("err create request")
			return nil, err
		}
		body, err := cl.Do(req)
		if err != nil {
			log.WithFields(log.Fields{"func": "(requester)Get", "err": err}).Errorf("err send request")
			return nil, err
		}
		defer body.Body.Close()
		page, err := NewPage(body.Body)
		if err != nil {
			log.WithFields(log.Fields{"func": "(requester)Get", "err": err}).Errorf("err create page")
			return nil, err
		}
		return page, nil
	}

}

//Crawler - интерфейс (контракт) краулера
type Crawler interface {
	Scan(ctx context.Context, url string, depth int64)
	ChanResult() <-chan CrawlResult
	AddDepth(Add int64) //добавляем дополнительный метод для увеличения глубины поиска
}

type crawler struct {
	r        Requester
	res      chan CrawlResult    //структура - либо ошибка либо данные по страничке.
	visited  map[string]struct{} //посещенные страницы.
	mu       sync.RWMutex
	maxDepth int64
}

func NewCrawler(r Requester, maxDepth int64) *crawler {

	return &crawler{
		r:        r,
		res:      make(chan CrawlResult),
		visited:  make(map[string]struct{}),
		mu:       sync.RWMutex{},
		maxDepth: maxDepth,
	}
}

func (c *crawler) Scan(ctx context.Context, url string, depth int64) {

	c.mu.RLock()
	if depth > c.maxDepth {
		log.WithFields(log.Fields{"func": "(crawler)Scan", "maxDepth": c.maxDepth}).Panicf("max depth was achieved")
		return
	}
	_, ok := c.visited[url] //Проверяем, что мы ещё не смотрели эту страницу
	c.mu.RUnlock()

	if ok {
		return
	}

	select {
	case <-ctx.Done(): //Если контекст завершен - прекращаем выполнение
		return
	default:
		page, err := c.r.Get(ctx, url) //Запрашиваем страницу через Requester
		if err != nil {
			c.res <- CrawlResult{Err: err} //Записываем ошибку в канал
			log.WithFields(log.Fields{"func": "(crawler)Scan", "url": url}).Error(err)
			return
		}
		c.mu.Lock()
		c.visited[url] = struct{}{} //Помечаем страницу просмотренной
		c.mu.Unlock()
		c.res <- CrawlResult{ //Отправляем результаты в канал
			Title: page.GetTitle(),
			Url:   url,
		}

		for _, link := range page.GetLinks() {
			go c.Scan(ctx, link, depth+1) //На все полученные ссылки запускаем новую рутину сборки
		}
	}
}

func (c *crawler) ChanResult() <-chan CrawlResult {
	return c.res
}

//Config - структура для конфигурации
type Config struct {
	Add        int64
	Depth      int64
	MaxDepth   int64
	MaxResults int
	MaxErrors  int
	Url        string
	Timeout    int //in seconds
}

func main() {

	cfg := Config{
		Add:        2,
		Depth:      0,
		MaxDepth:   3,
		MaxResults: 10000, //1000
		MaxErrors:  5000,  //500
		Url:        "https://telegram.org",
		Timeout:    10,
	}

	var r Requester
	var cr Crawler

	r = NewRequester(time.Duration(cfg.Timeout) * time.Second)

	cr = NewCrawler(r, cfg.MaxDepth)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cfg.Timeout)) //общий таймаут

	go cr.Scan(ctx, cfg.Url, cfg.Depth)    //Запускаем краулер в отдельной рутине
	go processResult(ctx, cancel, cr, cfg) //Обрабатываем результаты в отдельной рутине

	sigCh := make(chan os.Signal, 1)                      //Создаем канал для приема сигналов
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGUSR1) //Подписываемся на сигнал SIGINT

	for {
		select {
		case <-ctx.Done(): //Если всё завершили - выходим
			return
		case s := <-sigCh:
			switch s {
			case syscall.SIGINT:
				cancel()
				log.WithFields(log.Fields{"func": "main"}).Info("crawler aborted")
			case syscall.SIGUSR1:
				cr.AddDepth(cfg.Add)
				log.WithFields(log.Fields{
					"func":  "main",
					"add":   cfg.Add,
					"depth": cfg.MaxDepth,
				}).Info("The depth was increased")

			}
		}
	}

}

//получаем максимально возможную глубину
func (c *crawler) AddDepth(add int64) {
	atomic.AddInt64(&c.maxDepth, add)
}

func processResult(ctx context.Context, cancel func(), cr Crawler, cfg Config) {

	var maxResult, maxErrors = cfg.MaxResults, cfg.MaxErrors

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-cr.ChanResult():
			if msg.Err != nil {
				maxErrors--
				log.WithFields(log.Fields{"func": "processResult", "err": msg.Err.Error()}).Errorf("crawler result return err")
				if maxErrors <= 0 {
					cancel()
					return
				}
			} else {
				maxResult--
				log.WithFields(log.Fields{"func": "processResult", "title": msg.Title, "url": msg.Url}).Info("crawler result")
				if maxResult <= 0 {
					cancel()
					return
				}
			}
		}
	}
}
