// Producer: simulates F1 telemetry for a few drivers and publishes one JSON
// sample per driver per tick to the Kafka topic telemetry.raw.
// Prometheus scrapes its metrics on :8000/metrics (see prometheus/prometheus.yml).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

const topic = "telemetry.raw"

// Sample matches the columns of the telemetry table in sql/init.sql.
type Sample struct {
	Time     time.Time `json:"time"`
	Driver   string    `json:"driver"`
	Speed    float64   `json:"speed"`     // km/h
	Throttle float64   `json:"throttle"`  // 0-100 %
	Brake    float64   `json:"brake"`     // 0-100 %
	Gear     int       `json:"gear"`      // 1-8
	RPM      float64   `json:"rpm"`       // engine revs per minute
	TyreTemp float64   `json:"tyre_temp"` // degrees C
	TyreWear float64   `json:"tyre_wear"` // 0-100 %
}

// Car holds the state of one simulated car. Each car is used by a single
// goroutine, so it needs no lock.
type Car struct {
	driver     string
	rng        *rand.Rand
	speed      float64
	throttle   float64
	brakeTicks int // how many more ticks the car keeps braking
	brakeLevel float64
	tyreTemp   float64
	tyreWear   float64
}

func newCar(driver string, seed int64) *Car {
	return &Car{
		driver:   driver,
		rng:      rand.New(rand.NewSource(seed)),
		speed:    120,
		throttle: 60,
		tyreTemp: 80,
	}
}

// gearBands are the upper speed limits (km/h) of gears 1 to 8.
var gearBands = []float64{70, 110, 150, 190, 230, 270, 310, 360}

// gearFor returns the gear for a speed and how far (0-1) the speed is inside
// that gear's band, which drives the RPM.
func gearFor(speed float64) (gear int, frac float64) {
	low := 0.0
	for i, high := range gearBands {
		if speed < high || i == len(gearBands)-1 {
			return i + 1, clamp((speed-low)/(high-low), 0, 1)
		}
		low = high
	}
	return len(gearBands), 1
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// Step advances the car by one tick and returns the new sample. Speed moves
// smoothly (accelerate, then brake for a corner) so the charts look like a lap
// and not like noise.
func (c *Car) Step() Sample {
	brake := 0.0
	if c.brakeTicks > 0 {
		c.brakeTicks--
		brake = c.brakeLevel
		c.throttle = 0
		c.speed -= 0.6 * brake * (0.8 + 0.4*c.rng.Float64())
	} else {
		// random walk on the throttle, biased towards flat out
		c.throttle = clamp(c.throttle+(c.rng.Float64()-0.35)*30, 20, 100)
		c.speed += c.throttle * 0.35 * (0.8 + 0.4*c.rng.Float64())
		// start a braking zone from higher speeds
		if c.speed > 160 && c.rng.Float64() < 0.12 {
			c.brakeTicks = 2 + c.rng.Intn(3)
			c.brakeLevel = 60 + c.rng.Float64()*40
		}
	}
	c.speed = clamp(c.speed, 60, 345)

	gear, frac := gearFor(c.speed)
	rpm := 6500 + frac*5500 + c.rng.NormFloat64()*60

	// tyres: wear only goes up (faster under braking), temperature follows load
	c.tyreWear = math.Min(100, c.tyreWear+0.02+brake*0.0004)
	target := 70 + c.speed*0.22 + brake*0.25 + c.tyreWear*0.15
	c.tyreTemp += (target-c.tyreTemp)*0.1 + c.rng.NormFloat64()*0.3

	return Sample{
		Time:     time.Now().UTC(),
		Driver:   c.driver,
		Speed:    round1(c.speed),
		Throttle: round1(c.throttle),
		Brake:    round1(brake),
		Gear:     gear,
		RPM:      math.Round(rpm),
		TyreTemp: round1(c.tyreTemp),
		TyreWear: round2(c.tyreWear),
	}
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseDrivers splits a comma-separated list and drops empty names.
func parseDrivers(s string) []string {
	var out []string
	for _, name := range strings.Split(s, ",") {
		if name = strings.TrimSpace(name); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	metricsAddr := env("METRICS_ADDR", ":8000")
	drivers := parseDrivers(env("DRIVERS", "VER,HAM,LEC"))
	if len(drivers) == 0 {
		log.Fatal("DRIVERS must contain at least one driver name")
	}
	tickMS, err := strconv.Atoi(env("TICK_MS", "1000"))
	if err != nil || tickMS < 10 {
		log.Fatalf("TICK_MS must be a number of milliseconds >= 10, got %q", os.Getenv("TICK_MS"))
	}
	tick := time.Duration(tickMS) * time.Millisecond

	// --- Prometheus metrics ---
	sent := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "f1_samples_sent_total", Help: "Telemetry samples written to Kafka.",
	}, []string{"driver"})
	failed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "f1_send_errors_total", Help: "Telemetry samples that failed to reach Kafka.",
	}, []string{"driver"})
	speedGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "f1_last_speed_kmh", Help: "Last simulated speed per driver.",
	}, []string{"driver"})
	prometheus.MustRegister(sent, failed, speedGauge)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("metrics server: %v", err)
		}
	}()

	// --- Kafka writer ---
	// Key = driver, so all samples of one driver stay in order on one partition.
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- One goroutine per car, all feeding one channel ---
	samples := make(chan Sample, 256)
	var wg sync.WaitGroup
	for i, name := range drivers {
		car := newCar(name, time.Now().UnixNano()+int64(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(tick)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					select {
					case samples <- car.Step():
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() { wg.Wait(); close(samples) }()

	log.Printf("producing to %s on %v for drivers %v every %s; metrics on %s/metrics",
		topic, brokers, drivers, tick, metricsAddr)

	// --- Publisher: runs until the channel is closed and drained ---
	for s := range samples {
		body, err := json.Marshal(s)
		if err != nil {
			log.Printf("marshal: %v", err)
			continue
		}
		// separate timeout so a final flush still works after Ctrl+C
		wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = w.WriteMessages(wctx, kafka.Message{Key: []byte(s.Driver), Value: body})
		cancel()
		if err != nil {
			failed.WithLabelValues(s.Driver).Inc()
			log.Printf("kafka write (%s): %v", s.Driver, err)
			continue
		}
		sent.WithLabelValues(s.Driver).Inc()
		speedGauge.WithLabelValues(s.Driver).Set(s.Speed)
	}

	log.Println("shutting down")
	if err := w.Close(); err != nil {
		log.Printf("closing kafka writer: %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
