package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	logger "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	logger.SetFormatter(&logger.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	logger.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logger.SetLevel(logger.ErrorLevel)
}

type DatabaseDescriptor struct {
	username    string
	password    string
	dbName      string
	dbPort      string
	dbHostName  string
	dbPrecision string
}

func (d DatabaseDescriptor) getDatabaseURL() string {
	return "http://" + d.dbHostName + ":" + d.dbPort + "/write?db=" + d.dbName + "&precision=" + d.dbPrecision
}

// Metric represents a point that's going to be stored on influx db
// <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
type Metric struct {
	measurement string
	tags        map[string]string
	fields      map[string]string
	timestamp   int64
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (m Metric) flatten() string {
	var b bytes.Buffer
	b.WriteString(m.measurement)
	b.WriteString(",")

	tagsSlice := make([]string, 0)
	fieldsSlice := make([]string, 0)

	for t, v1 := range m.tags {
		tagsSlice = append(tagsSlice, fmt.Sprintf("%s=%s", t, v1))
	}
	b.WriteString(strings.Join(tagsSlice, ","))

	b.WriteString(" ")

	for f, v2 := range m.fields {
		fieldsSlice = append(fieldsSlice, fmt.Sprintf("%s=%s", f, v2))
	}
	b.WriteString(strings.Join(fieldsSlice, ","))
	b.WriteString(" ")

	b.WriteString(strconv.FormatInt(m.timestamp, 10))
	return b.String()
}

func (m Metric) publishToDB(dbURL string) {
	metricData := m.flatten()
	logger.Infof("received: %s", metricData)
	body := strings.NewReader(metricData)
	req, err := http.NewRequest("POST", dbURL, body)
	if err != nil {
		logger.Error("Could not create new post request to influx")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}

	resp, err := netClient.Do(req)
	if err != nil {
		logger.Errorf("%v", err)
	}
	if resp != nil {
		// defer the closing of our inputJSONFile so that we can parse it later on
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				logger.Errorf("failed to close the file %v", err)
			}
		}()
	}
}

func degreeToRads(degree float64) float64 {
	return degree * (math.Pi / 180)
}

func signalProducer(in chan float64) {
	for i := 0; i < 360; i++ {
		in <- math.Sin(degreeToRads(float64(i)))
	}
	close(in)
}

func metricsProducer(in chan float64, out chan Metric, tags map[string]string) {
	for value := range in {
		logger.Debug("Generating Metric for value: %.4f", value)

		signal := fmt.Sprintf("%.4f", value)

		mt := Metric{
			measurement: "weather",
			tags:        tags,
			fields:      map[string]string{"temperature": signal},
			timestamp:   makeTimestamp(),
		}

		out <- mt
	}
	close(out)
}

func metricsPublisher(metrics chan Metric, dbURL string) {
	for metric := range metrics {
		metric.publishToDB(dbURL)
	}
}

func main() {

	// put values into channel
	signalValues := make(chan float64)
	metrics := make(chan Metric)

	go signalProducer(signalValues)

	tags := map[string]string{"location": "colombia", "aggregator": "agg-col-001"}
	go metricsProducer(signalValues, metrics, tags)

	// get the values from the channel, generate metrics and publish the metrics
	database := DatabaseDescriptor{dbHostName: "localhost", dbPort: "8086", dbName: "iot_field_metrics", dbPrecision: "ms"}

	databaseURL := database.getDatabaseURL()

	logger.Info("Ready to send data to %s", databaseURL)

	metricsPublisher(metrics, databaseURL)
}
