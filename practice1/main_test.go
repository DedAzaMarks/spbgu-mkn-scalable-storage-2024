package main

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func TestAPI(t *testing.T) {
	mux := http.NewServeMux()

	// Инициализация Storage и Router
	storage := NewStorage(mux, "storage", "test_geo.db.json")
	router := NewRouter(mux, [][]string{{"storage"}})
	storage.Run()
	router.Run()
	t.Cleanup(func() {
		storage.Stop()
		router.Stop()
	})

	// Табличный тест
	tests := []struct {
		name       string
		method     string
		url        string
		body       func() []byte
		statusCode int
	}{
		{
			name:   "Insert feature",
			method: "POST",
			url:    "/insert",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				feature.ID = "test-id-1"
				data, _ := feature.MarshalJSON()
				return data
			},
			statusCode: http.StatusOK,
		},
		{
			name:   "Replace feature",
			method: "POST",
			url:    "/replace",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				feature.ID = "test-id-1"
				data, _ := feature.MarshalJSON()
				return data
			},
			statusCode: http.StatusOK,
		},
		{
			name:   "Delete feature",
			method: "POST",
			url:    "/delete",
			body: func() []byte {
				data, _ := json.Marshal(map[string]string{"id": "test-id-1"})
				return data
			},
			statusCode: http.StatusOK,
		},
		{
			name:       "Select all features",
			method:     "GET",
			url:        "/select",
			body:       nil,
			statusCode: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Создаем запрос
			var body *bytes.Reader
			if test.body != nil {
				body = bytes.NewReader(test.body())
			} else {
				body = bytes.NewReader(nil)
			}

			req, err := http.NewRequest(test.method, test.url, body)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			// Выполняем запрос
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			// Проверяем код ответа
			if rec.Code == http.StatusTemporaryRedirect {
				// Следуем за редиректом
				location := rec.Header().Get("Location")
				req, err = http.NewRequest(test.method, location, body)
				if err != nil {
					t.Fatalf("failed to create redirect request: %v", err)
				}
				rec = httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
			}

			if rec.Code != test.statusCode {
				t.Errorf("unexpected status code: got %v, want %v", rec.Code, test.statusCode)
			}

			// Дополнительно проверяем содержимое ответа для `select`
			if test.name == "Select all features" && rec.Code == http.StatusOK {
				var collection geojson.FeatureCollection
				if err := json.NewDecoder(rec.Body).Decode(&collection); err != nil {
					t.Errorf("failed to decode response: %v", err)
				}
			}
		})
	}
}
