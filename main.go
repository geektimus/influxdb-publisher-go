package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"
)

//http://localhost:8086/write?db=mydb

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
	timestamp   time.Time
}

func (m Metric) copyMetric() Metric {
	newMetric := Metric{}
	if err := copier.Copy(&newMetric, m); err != nil {
		logrus.Error("Could not copy the metrics")
	}
	return newMetric
}

func (m Metric) flatten() string {
	var b bytes.Buffer
	b.WriteString(m.measurement)
	for t, v := range m.tags {
		b.WriteString(fmt.Sprintf("%s=%s", t, v))
	}
	for f, v := range m.tags {
		b.WriteString(fmt.Sprintf("%s=%s", f, v))
	}
	b.WriteString(m.timestamp.String())
	return b.String()
}

func (m Metric) publishToDB(dbURL string) {
	body := strings.NewReader(m.flatten())
	req, err := http.NewRequest("POST", dbURL, body)
	if err != nil {
		logrus.Error("Could not create new post request to influx")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logrus.Error("Could not send the request to influx")
	}

	// defer the closing of our inputJSONFile so that we can parse it later on
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Errorf("failed to close the file %v", err)
		}
	}()
}

func degreeToRads(degree float64) float64 {
	return degree * (math.Pi / 180)
}

func main() {

	// put values into channel
	c := make(chan float64)

	go func(a chan float64) {
		for i := 0; i <= 360; i++ {
			a <- math.Sin(degreeToRads(float64(i)))
		}
		close(a)
	}(c)

	// get the values from the channel, generate metrics and publish the metrics
	database := DatabaseDescriptor{dbHostName: "localhost", dbPort: "8086", dbName: "iot_field_metrics", dbPrecision: "ms"}

	databaseURL := database.getDatabaseURL()

	logrus.Info("Ready to send data to %s", databaseURL)

	tags := map[string]string{"location": "colombia", "aggregator": "agg-col-001"}

	m := make(chan Metric)

	go func(values chan float64, metrics chan Metric) {
		value := <-values

		logrus.Infof("Generating Metric for value: %.4f", value)

		signal := fmt.Sprintf("%.4f", value)

		mt := Metric{
			measurement: "weather",
			tags:        tags,
			fields:      map[string]string{"temperature": signal},
			timestamp:   time.Now(),
		}

		metrics <- mt
	}(c, m)

	<-c
	<-m

	//go func() {
	//	metric := <- m
	//	logrus.Info("Processing Metric: %v", metric)
	//	metric.publishToDB(databaseURL)
	//}()

}
