// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/cli"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/soheilhy/cmux"
	"github.com/spf13/pflag"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	sync.RWMutex

	tcpAddr      string
	runningTasks map[string]*export.Dumper
}

// listen return the listener from tls.Listen if tlsConfig is NOT Nil.
func listen(network, addr string, tlsConfig *tls.Config) (listener net.Listener, err error) {
	URL, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid listening socket addr (%s)", addr)
	}

	if tlsConfig != nil {
		listener, err = tls.Listen(network, URL.Host, tlsConfig)
		if err != nil {
			return nil, errors.Annotatef(err, "fail to start %s on %s", network, URL.Host)
		}
	} else {
		listener, err = net.Listen(network, URL.Host)
		if err != nil {
			return nil, errors.Annotatef(err, "fail to start %s on %s", network, URL.Host)
		}
	}

	return listener, nil
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	taskName := mux.Vars(r)["taskName"]
	dumpers := make(map[string]*export.Dumper, 0)
	s.RLock()
	for name, dumper := range s.runningTasks {
		if taskName != "" && name != taskName {
			continue
		}
		dumpers[name] = dumper
	}
	s.RUnlock()
	dumperStatuses := make([]*export.DumpStatus, 0, len(dumpers))
	for name, dumper := range dumpers {
		dumperStatus := dumper.GetStatus()
		if err := dumper.Error.Load(); err != nil {
			dumperStatus.Error = err.Error()
		}
		dumperStatus.Task = name
		dumperStatuses = append(dumperStatuses, dumperStatus)
	}
	if err := json.NewEncoder(w).Encode(dumperStatuses); err != nil {
		log.Error("Failed to encode dump statuses", zap.Error(err), zap.Any("dumpers", dumperStatuses))
	}
}

func (s *Server) startTask(w http.ResponseWriter, r *http.Request) {
	taskName := mux.Vars(r)["taskName"]
	if taskName == "" {
		w.Write([]byte(`{"error": "task name is empty"}`))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, `{"error": "failed to read body, err: %s"}`, err)
		return
	}
	conf := export.DefaultConfig()
	err = json.Unmarshal(body, conf)
	if err != nil {
		fmt.Fprintf(w, `{"error": "failed to unmarshal configuration, err: %s"}`, err)
		return
	}
	go func() {
		conf.Logger = log.L().With(zap.String("task", taskName))
		dumper, err := export.NewDumper(context.TODO(), conf)
		if err != nil {
			s.Lock()
			s.runningTasks[taskName] = &export.Dumper{Error: *atomic.NewError(err)}
			s.Unlock()
			return
		}
		s.Lock()
		s.runningTasks[taskName] = dumper
		s.Unlock()
		err = dumper.Dump()
		_ = dumper.Close()
		if err != nil {
			dumper.Error.Store(err)
		}
	}()
	w.Write([]byte(`{"msg": "success"}`))
}

func (s *Server) stopTask(w http.ResponseWriter, r *http.Request) {
	taskName := mux.Vars(r)["taskName"]
	if taskName == "" {
		w.Write([]byte(`{"error": "task name is empty"}`))
		return
	}
	s.RLock()
	dumper, ok := s.runningTasks[taskName]
	s.RUnlock()
	if !ok {
		fmt.Fprintf(w, `{"msg": "task %s is not found"}`, taskName)
		return
	}
	_ = dumper.Close()
	s.Lock()
	delete(s.runningTasks, taskName)
	s.Unlock()
	w.Write([]byte(`{"msg": "success"}`))
}

func (s *Server) start() error {
	// start a TCP listener
	// we need to manage TLS here for cmux to distinguish between HTTP and gRPC.
	tcpLis, err := listen("tcp", s.tcpAddr, nil)
	if err != nil {
		return errors.Trace(err)
	}
	// grpc and http will use the same tcp connection
	m := cmux.New(tcpLis)
	// sets a timeout for the read of matchers
	m.SetReadTimeout(time.Second * 10)

	httpL := m.Match(cmux.HTTP1Fast())
	router := mux.NewRouter()
	router.HandleFunc("/tasks/{taskName}", s.listTasks).Methods("GET")
	router.HandleFunc("/tasks/{taskName}", s.startTask).Methods("POST")
	router.HandleFunc("/tasks/{taskName}", s.stopTask).Methods("DELETE")
	http.Handle("/", router)

	go func() {
		err := http.Serve(httpL, nil)
		if err != nil {
			log.Info("HTTP server stopped", zap.Error(err))
		}
	}()

	log.Info("start to server request", zap.String("addr", s.tcpAddr))
	err = m.Serve()
	if strings.Contains(err.Error(), "use of closed network connection") {
		err = nil
	}
	return err
}

func main() {
	pflag.Usage = func() {
		pflag.PrintDefaults()
	}
	printVersion := pflag.BoolP("version", "V", false, "Print Dumpling version")
	pflag.CommandLine.Bool("help", false, "Print help message and quit")
	pflag.CommandLine.String("addr", "127.0.0.1:8283", "addr(i.e. 'host:port') to listen on for client traffic")
	pflag.Parse()
	log.InitLogger(&log.Config{Level: "info", File: log.FileLogConfig{}})
	if printHelp, err := pflag.CommandLine.GetBool(export.FlagHelp); printHelp || err != nil {
		if err != nil {
			fmt.Printf("\nGet help flag error: %s\n", err)
		}
		pflag.Usage()
		return
	}
	println(cli.LongVersion())
	if *printVersion {
		return
	}

	listenAddr, err := pflag.CommandLine.GetString("addr")
	if err != nil {
		fmt.Printf("\nparse arguments failed: %+v\n", err)
		os.Exit(1)
	}
	if pflag.NArg() > 0 {
		fmt.Printf("\nmeet some unparsed arguments, please check again: %+v\n", pflag.Args())
		os.Exit(1)
	}
	listenAddr = "http://" + listenAddr

	s := &Server{
		tcpAddr:      listenAddr,
		runningTasks: make(map[string]*export.Dumper),
	}
	err = s.start()
	if err != nil {
		fmt.Printf("\nmeet some err, please check again: %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("dumpling server will exit now")
}
