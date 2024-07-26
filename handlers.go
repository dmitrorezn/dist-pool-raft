package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
)

func add(s *Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var tx domain.Transaction
		if err := json.NewDecoder(request.Body).Decode(&tx); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}
		fmt.Println("add tx", tx)

		leader, err := s.Add(request.Context(), &tx)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}
		if leader != "" {
			fmt.Println("URL", "http://"+leader+request.RequestURI)
			http.Redirect(writer, request, "http://"+leader+request.RequestURI, http.StatusOK)

			return
		}

		writer.WriteHeader(http.StatusOK)
	}
}

func list(s *Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		txx, err := s.List(request.Context())
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}
		fmt.Println("txx", txx)
		writer.WriteHeader(http.StatusOK)
		if len(txx) > 0 {
			_ = json.NewEncoder(writer).Encode(txx)
		}
	}
}
