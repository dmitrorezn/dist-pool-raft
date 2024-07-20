package main

import (
	"encoding/json"
	"net/http"

	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
)

func add(s *Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var tx domain.Transaction
		err := json.NewDecoder(request.Body).Decode(&tx)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}
		if err = s.Add(request.Context(), &tx); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

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

		writer.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(writer).Encode(txx)

	}
}
