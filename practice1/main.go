package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/paulmach/orb/geojson"
)

var c = jsoniter.Config{
	EscapeHTML:              true,
	SortMapKeys:             false,
	MarshalFloatWith6Digits: true,
}.Froze()

func init() {
	geojson.CustomJSONMarshaler = c
	geojson.CustomJSONUnmarshaler = c
}

// Router реализует перенаправление HTTP запросов
type Router struct {
	mux   *http.ServeMux
	nodes [][]string
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	// is it replica??? why are nodes are in form of table of strings
	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	for _, row := range nodes {
		for _, node := range row {
			mux.Handle("/insert", http.RedirectHandler("/"+node+"/insert", http.StatusTemporaryRedirect))
			mux.Handle("/replace", http.RedirectHandler("/"+node+"/replace", http.StatusTemporaryRedirect))
			mux.Handle("/delete", http.RedirectHandler("/"+node+"/delete", http.StatusTemporaryRedirect))
			mux.Handle("/select", http.RedirectHandler("/"+node+"/select", http.StatusTemporaryRedirect))
		}
	}
	return &Router{
		mux:   mux,
		nodes: nodes,
	}
}

func (r *Router) Run() {
	slog.Info("Router started")
}

func (r *Router) Stop() {
	slog.Info("Router stopped")
}

// Storage управляет сохранением GeoJSON объектов
type Storage struct {
	mux    *http.ServeMux
	name   string
	geoDB  map[string]*geojson.Feature
	mutex  sync.RWMutex
	dbFile string
}

func NewStorage(mux *http.ServeMux, name string, dbFile string) *Storage {
	storage := &Storage{
		mux:    mux,
		name:   name,
		geoDB:  make(map[string]*geojson.Feature),
		dbFile: dbFile,
	}

	mux.HandleFunc("/"+name+"/insert", storage.insertHandler)
	mux.HandleFunc("/"+name+"/replace", storage.replaceHandler)
	mux.HandleFunc("/"+name+"/delete", storage.deleteHandler)
	mux.HandleFunc("/"+name+"/select", storage.selectHandler)

	return storage
}

func (s *Storage) Run() {
	s.loadFromFile()
	slog.Info("Storage started", "name", s.name)
}

func (s *Storage) Stop() {
	s.saveToFile()
	slog.Info("Storage stopped", "name", s.name)
}

func (s *Storage) loadFromFile() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	file, err := os.Open(s.dbFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		slog.Error("Failed to load DB", "err", err)
		return
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&s.geoDB); err != nil {
		slog.Error("Failed to decode DB", "err", err)
	}
}

func (s *Storage) saveToFile() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	file, err := os.Create(s.dbFile)
	if err != nil {
		slog.Error("Failed to save DB", "err", err)
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, f := range s.geoDB {
		f.MarshalJSON()
	}
	if err := encoder.Encode(s.geoDB); err != nil {
		slog.Error("Failed to encode DB", "err", err)
	}
}

func checkID(w http.ResponseWriter, feature *geojson.Feature) (string, bool) {
	id, ok := feature.ID.(string)
	if !ok {
		slog.Error("ID is wrong type, string expected")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("ID is wrong type, string expected"))
		return "", false
	}
	return id, true
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
	buf, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		slog.Error("/insert read body", slog.Any("error", err.Error()))
	}
	feature, err := geojson.UnmarshalFeature(buf)
	if err != nil {
		http.Error(w, "invalid geojson", http.StatusBadRequest)
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id, ok := checkID(w, feature)
	if !ok {
		return
	}
	s.geoDB[id] = feature
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	buf, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		slog.Error("/replace read body", slog.Any("error", err.Error()))
	}
	feature, err := geojson.UnmarshalFeature(buf)
	if err != nil {
		http.Error(w, "invalid geojson", http.StatusBadRequest)
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id, ok := checkID(w, feature)
	if !ok {
		return
	}
	if _, exists := s.geoDB[id]; !exists {
		http.Error(w, "feature not found", http.StatusNotFound)
		return
	}
	s.geoDB[id] = feature
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	var data struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.geoDB, data.ID)
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	collection := geojson.NewFeatureCollection()
	for _, feature := range s.geoDB {
		collection.Append(feature)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(collection)
}

func main() {
	mux := http.NewServeMux()

	// Создаем компоненты
	storage := NewStorage(mux, "storage", "geo.db.json")
	router := NewRouter(mux, [][]string{{"storage"}})

	storage.Run()
	router.Run()

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	// Завершаем сервер по сигналу
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		server.Shutdown(ctx)
		storage.Stop()
		router.Stop()
	}()

	slog.Info("Server running at http://127.0.0.1:8080")
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Server error", "err", err)
	}
}
